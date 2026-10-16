//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	diffMagic            = "ADIF"
	diffVersion   uint32 = 1
	diffPageSize  int64  = 4096
	diffChunkSize int64  = 256 * 1024
)

// SparseExtent describes a contiguous byte range [Offset, Offset+Length) in a
// guest memory image.
type SparseExtent struct {
	Offset int64
	Length int64
}

// DeflateAgainstBase compares populated regions of targetFile against baseFile
// at 4 KiB page granularity and punches holes (FALLOC_FL_PUNCH_HOLE |
// FALLOC_FL_KEEP_SIZE) in targetFile wherever pages are byte-identical to
// baseFile. It returns the number of bytes punched out as holes and the number
// of modified bytes kept.
func DeflateAgainstBase(ctx context.Context, baseFile, targetFile string) (punchedBytes, keptBytes int64, err error) {
	baseF, err := os.Open(baseFile)
	if err != nil {
		return 0, 0, fmt.Errorf("open base %s: %w", baseFile, err)
	}
	defer baseF.Close()

	targetF, err := os.OpenFile(targetFile, os.O_RDWR, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("open target %s: %w", targetFile, err)
	}
	defer targetF.Close()

	baseFi, err := baseF.Stat()
	if err != nil {
		return 0, 0, err
	}
	targetFi, err := targetF.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := targetFi.Size()
	if baseFi.Size() != size {
		return 0, 0, fmt.Errorf("base/target size mismatch (%d vs %d)", baseFi.Size(), size)
	}

	baseFd := int(baseF.Fd())
	targetFd := int(targetF.Fd())

	const chunkSize = 64 * 1024
	const pageSize = 4096
	baseBuf := make([]byte, chunkSize)
	targetBuf := make([]byte, chunkSize)

	off := int64(0)
	for off < size {
		if err := ctx.Err(); err != nil {
			return punchedBytes, keptBytes, err
		}
		dataStart, err := unix.Seek(targetFd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break
			}
			return punchedBytes, keptBytes, fmt.Errorf("SEEK_DATA on target: %w", err)
		}
		dataEnd, err := unix.Seek(targetFd, dataStart, unix.SEEK_HOLE)
		if err != nil {
			return punchedBytes, keptBytes, fmt.Errorf("SEEK_HOLE on target: %w", err)
		}

		var punchStart, punchLen int64
		flushPunch := func() error {
			if punchLen <= 0 {
				return nil
			}
			if err := unix.Fallocate(targetFd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, punchStart, punchLen); err != nil {
				return fmt.Errorf("fallocate punch hole at %d len %d: %w", punchStart, punchLen, err)
			}
			punchedBytes += punchLen
			punchLen = 0
			return nil
		}

		for pos := dataStart; pos < dataEnd; {
			if err := ctx.Err(); err != nil {
				return punchedBytes, keptBytes, err
			}
			n := int64(len(targetBuf))
			if rem := dataEnd - pos; rem < n {
				n = rem
			}
			if err := preadFull(targetFd, targetBuf[:n], pos); err != nil {
				return punchedBytes, keptBytes, fmt.Errorf("read target at %d: %w", pos, err)
			}
			if err := preadFull(baseFd, baseBuf[:n], pos); err != nil {
				return punchedBytes, keptBytes, fmt.Errorf("read base at %d: %w", pos, err)
			}

			for p := int64(0); p < n; p += pageSize {
				pLen := int64(pageSize)
				if rem := n - p; rem < pLen {
					pLen = rem
				}
				if bytes.Equal(targetBuf[p:p+pLen], baseBuf[p:p+pLen]) {
					if punchLen == 0 {
						punchStart = pos + p
					}
					punchLen += pLen
				} else {
					if err := flushPunch(); err != nil {
						return punchedBytes, keptBytes, err
					}
					keptBytes += pLen
				}
			}
			pos += n
		}
		if err := flushPunch(); err != nil {
			return punchedBytes, keptBytes, err
		}
		off = dataEnd
	}

	return punchedBytes, keptBytes, nil
}

// CreateDeltaAgainstBase compares populated extents of targetFile against
// baseFile at 4 KiB page granularity and writes only modified extents into
// metaFile (ADIF v1 header + extent table) and dataFile (contiguous dirty-page
// payload). It returns the total dirty byte count written to dataFile.
func CreateDeltaAgainstBase(ctx context.Context, baseFile, targetFile, metaFile, dataFile string) (keptBytes int64, err error) {
	baseF, err := os.Open(baseFile)
	if err != nil {
		return 0, fmt.Errorf("open base %s: %w", baseFile, err)
	}
	defer baseF.Close()

	targetF, err := os.Open(targetFile)
	if err != nil {
		return 0, fmt.Errorf("open target %s: %w", targetFile, err)
	}
	defer targetF.Close()

	baseFi, err := baseF.Stat()
	if err != nil {
		return 0, err
	}
	targetFi, err := targetF.Stat()
	if err != nil {
		return 0, err
	}
	size := targetFi.Size()
	if baseFi.Size() != size {
		return 0, fmt.Errorf("base/target size mismatch (%d vs %d)", baseFi.Size(), size)
	}

	dataF, err := os.OpenFile(dataFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, fmt.Errorf("create delta data %s: %w", dataFile, err)
	}
	defer func() {
		if err != nil {
			dataF.Close()
			os.Remove(dataFile)
		}
	}()
	dataBw := bufio.NewWriterSize(dataF, 1<<20)

	baseFd := int(baseF.Fd())
	targetFd := int(targetF.Fd())
	baseBuf := make([]byte, diffChunkSize)
	targetBuf := make([]byte, diffChunkSize)

	var extents []SparseExtent

	off := int64(0)
	for off < size {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		dataStart, serr := unix.Seek(targetFd, off, unix.SEEK_DATA)
		if serr != nil {
			if errors.Is(serr, unix.ENXIO) {
				break
			}
			return 0, fmt.Errorf("SEEK_DATA on target: %w", serr)
		}
		dataEnd, serr := unix.Seek(targetFd, dataStart, unix.SEEK_HOLE)
		if serr != nil {
			return 0, fmt.Errorf("SEEK_HOLE on target: %w", serr)
		}

		for pos := dataStart; pos < dataEnd; {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			n := diffChunkSize
			if rem := dataEnd - pos; rem < n {
				n = rem
			}
			if err := preadFull(targetFd, targetBuf[:n], pos); err != nil {
				return 0, fmt.Errorf("read target at %d: %w", pos, err)
			}
			if err := preadFull(baseFd, baseBuf[:n], pos); err != nil {
				return 0, fmt.Errorf("read base at %d: %w", pos, err)
			}

			for p := int64(0); p < n; p += diffPageSize {
				pLen := diffPageSize
				if rem := n - p; rem < pLen {
					pLen = rem
				}
				if !bytes.Equal(targetBuf[p:p+pLen], baseBuf[p:p+pLen]) {
					pageOff := pos + p
					if len(extents) > 0 {
						last := &extents[len(extents)-1]
						if last.Offset+last.Length == pageOff {
							last.Length += pLen
						} else {
							extents = append(extents, SparseExtent{Offset: pageOff, Length: pLen})
						}
					} else {
						extents = append(extents, SparseExtent{Offset: pageOff, Length: pLen})
					}
					if _, werr := dataBw.Write(targetBuf[p : p+pLen]); werr != nil {
						return 0, fmt.Errorf("write delta data: %w", werr)
					}
					keptBytes += pLen
				}
			}
			pos += n
		}
		off = dataEnd
	}

	if err := dataBw.Flush(); err != nil {
		return 0, fmt.Errorf("flush delta data: %w", err)
	}
	if err := dataF.Close(); err != nil {
		return 0, fmt.Errorf("close delta data: %w", err)
	}

	metaF, err := os.OpenFile(metaFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, fmt.Errorf("create delta meta %s: %w", metaFile, err)
	}
	defer func() {
		if err != nil {
			metaF.Close()
			os.Remove(metaFile)
		}
	}()

	header := make([]byte, 24+len(extents)*16)
	copy(header[0:4], diffMagic)
	binary.LittleEndian.PutUint32(header[4:8], diffVersion)
	binary.LittleEndian.PutUint64(header[8:16], uint64(size))
	binary.LittleEndian.PutUint64(header[16:24], uint64(len(extents)))
	for i, ext := range extents {
		base := 24 + i*16
		binary.LittleEndian.PutUint64(header[base:base+8], uint64(ext.Offset))
		binary.LittleEndian.PutUint64(header[base+8:base+16], uint64(ext.Length))
	}
	if _, err := metaF.Write(header); err != nil {
		return 0, fmt.Errorf("write delta meta: %w", err)
	}
	if err := metaF.Close(); err != nil {
		return 0, fmt.Errorf("close delta meta: %w", err)
	}

	return keptBytes, nil
}

// parseDeltaMeta parses an ADIF v1 metadata buffer and returns the target file
// logical size and its dirty extents.
func parseDeltaMeta(metaBytes []byte) (targetSize int64, extents []SparseExtent, err error) {
	if len(metaBytes) < 24 || string(metaBytes[0:4]) != diffMagic {
		return 0, nil, fmt.Errorf("invalid delta meta header")
	}
	ver := binary.LittleEndian.Uint32(metaBytes[4:8])
	if ver != diffVersion {
		return 0, nil, fmt.Errorf("unsupported delta version %d", ver)
	}
	targetSize = int64(binary.LittleEndian.Uint64(metaBytes[8:16]))
	numExtents := int(binary.LittleEndian.Uint64(metaBytes[16:24]))
	if targetSize < 0 || numExtents < 0 || len(metaBytes) != 24+numExtents*16 {
		return 0, nil, fmt.Errorf("corrupt delta meta size (%d bytes for %d extents)", len(metaBytes), numExtents)
	}
	extents = make([]SparseExtent, numExtents)
	for i := 0; i < numExtents; i++ {
		base := 24 + i*16
		extents[i] = SparseExtent{
			Offset: int64(binary.LittleEndian.Uint64(metaBytes[base : base+8])),
			Length: int64(binary.LittleEndian.Uint64(metaBytes[base+8 : base+16])),
		}
	}
	return targetSize, extents, nil
}

// RestoreFromDelta reconstructs outFile by sparsely copying baseFile and
// overlaying the dirty extents described by metaFile and dataFile.
func RestoreFromDelta(ctx context.Context, baseFile, metaFile, dataFile, outFile string) error {
	tmpFile := outFile + ".reconstructed"
	_ = os.Remove(tmpFile)
	if err := CopySparseFileFast(ctx, baseFile, tmpFile); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("copy base for delta restore: %w", err)
	}
	if _, _, err := OverlayDeltaFilesRecorded(ctx, metaFile, dataFile, tmpFile); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}
	_ = os.Remove(outFile)
	if err := os.Rename(tmpFile, outFile); err != nil {
		_ = os.Remove(tmpFile)
		return err
	}
	return nil
}

// OverlayDeltaFilesRecorded overlays the dirty extents from an ADIF v1
// metaFile + dataFile pair onto an existing dstFile in place, returning the
// modified extents and total overlaid byte count.
func OverlayDeltaFilesRecorded(ctx context.Context, metaFile, dataFile, dstFile string) ([]SparseExtent, int64, error) {
	metaBytes, err := os.ReadFile(metaFile)
	if err != nil {
		return nil, 0, fmt.Errorf("read delta meta %s: %w", metaFile, err)
	}
	targetSize, extents, err := parseDeltaMeta(metaBytes)
	if err != nil {
		return nil, 0, err
	}

	dataF, err := os.Open(dataFile)
	if err != nil {
		return nil, 0, fmt.Errorf("open delta data %s: %w", dataFile, err)
	}
	defer dataF.Close()

	dstF, err := os.OpenFile(dstFile, os.O_WRONLY, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("open dst %s: %w", dstFile, err)
	}
	defer dstF.Close()

	dstFi, err := dstF.Stat()
	if err != nil {
		return nil, 0, err
	}
	if dstFi.Size() != targetSize {
		return nil, 0, fmt.Errorf("dst size mismatch (%d vs %d)", dstFi.Size(), targetSize)
	}

	dataFd := int(dataF.Fd())
	dstFd := int(dstF.Fd())
	buf := make([]byte, diffChunkSize)
	dataPos := int64(0)
	var totalBytes int64

	for _, ext := range extents {
		if err := ctx.Err(); err != nil {
			return nil, totalBytes, err
		}
		if ext.Offset < 0 || ext.Length < 0 || ext.Offset > targetSize || ext.Length > targetSize-ext.Offset {
			return nil, totalBytes, fmt.Errorf("delta extent out of bounds (off=%d len=%d size=%d)", ext.Offset, ext.Length, targetSize)
		}
		rem := ext.Length
		dstOff := ext.Offset
		for rem > 0 {
			rOff := dataPos
			wOff := dstOff
			n, cerr := unix.CopyFileRange(dataFd, &rOff, dstFd, &wOff, int(rem), 0)
			if cerr != nil {
				if errors.Is(cerr, unix.EINTR) {
					continue
				}
				if errors.Is(cerr, unix.EXDEV) || errors.Is(cerr, unix.ENOSYS) || errors.Is(cerr, unix.EINVAL) {
					for rem > 0 {
						chunk := int64(len(buf))
						if rem < chunk {
							chunk = rem
						}
						if err := preadFull(dataFd, buf[:chunk], dataPos); err != nil {
							return nil, totalBytes, fmt.Errorf("read delta data at %d: %w", dataPos, err)
						}
						if err := pwriteFull(dstFd, buf[:chunk], dstOff); err != nil {
							return nil, totalBytes, fmt.Errorf("write dst at %d: %w", dstOff, err)
						}
						dataPos += chunk
						dstOff += chunk
						totalBytes += chunk
						rem -= chunk
					}
					break
				}
				return nil, totalBytes, fmt.Errorf("copy_file_range delta at %d: %w", dstOff, cerr)
			}
			if n == 0 {
				return nil, totalBytes, io.ErrUnexpectedEOF
			}
			dataPos = rOff
			dstOff = wOff
			totalBytes += int64(n)
			rem -= int64(n)
		}
	}

	return extents, totalBytes, nil
}

// CopySparseFileFast copies a sparse file preserving holes by iterating
// SEEK_DATA/SEEK_HOLE extents and invoking copy_file_range (falling back to
// pread/pwrite when copy_file_range is unavailable or crosses filesystems).
func CopySparseFileFast(ctx context.Context, src, dst string) error {
	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()

	fi, err := sf.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	df, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fi.Mode().Perm())
	if err != nil {
		return err
	}
	defer df.Close()

	if err := df.Truncate(size); err != nil {
		return err
	}

	sfd := int(sf.Fd())
	dfd := int(df.Fd())
	var fallbackBuf []byte

	off := int64(0)
	for off < size {
		if err := ctx.Err(); err != nil {
			return err
		}
		dataStart, err := unix.Seek(sfd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break
			}
			return fmt.Errorf("SEEK_DATA on %s: %w", src, err)
		}
		dataEnd, err := unix.Seek(sfd, dataStart, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("SEEK_HOLE on %s: %w", src, err)
		}

		for pos := dataStart; pos < dataEnd; {
			if err := ctx.Err(); err != nil {
				return err
			}
			rem := dataEnd - pos
			rOff := pos
			wOff := pos
			n, cerr := unix.CopyFileRange(sfd, &rOff, dfd, &wOff, int(rem), 0)
			if cerr != nil {
				if errors.Is(cerr, unix.EINTR) {
					continue
				}
				if errors.Is(cerr, unix.EXDEV) || errors.Is(cerr, unix.ENOSYS) || errors.Is(cerr, unix.EINVAL) {
					if fallbackBuf == nil {
						fallbackBuf = make([]byte, diffChunkSize)
					}
					for pos < dataEnd {
						chunk := int64(len(fallbackBuf))
						if r := dataEnd - pos; r < chunk {
							chunk = r
						}
						if err := preadFull(sfd, fallbackBuf[:chunk], pos); err != nil {
							return err
						}
						if err := pwriteFull(dfd, fallbackBuf[:chunk], pos); err != nil {
							return err
						}
						pos += chunk
					}
					break
				}
				return fmt.Errorf("copy_file_range on %s at %d: %w", src, pos, cerr)
			}
			if n == 0 {
				break
			}
			pos = rOff
		}
		off = dataEnd
	}
	return nil
}

// OverlaySparseRegionsRecorded overlays all populated SEEK_DATA/SEEK_HOLE
// regions from deltaFile onto an existing dstFile in place and returns the
// list of modified extents along with the total overlaid byte count.
func OverlaySparseRegionsRecorded(ctx context.Context, deltaFile, dstFile string) ([]SparseExtent, int64, error) {
	df, err := os.Open(deltaFile)
	if err != nil {
		return nil, 0, fmt.Errorf("open delta %s: %w", deltaFile, err)
	}
	defer df.Close()

	tf, err := os.OpenFile(dstFile, os.O_WRONLY, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("open target %s: %w", dstFile, err)
	}
	defer tf.Close()

	dfi, err := df.Stat()
	if err != nil {
		return nil, 0, err
	}
	tfi, err := tf.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := dfi.Size()
	if tfi.Size() != size {
		return nil, 0, fmt.Errorf("delta/target size mismatch (%d vs %d)", size, tfi.Size())
	}

	dfd := int(df.Fd())
	tfd := int(tf.Fd())
	buf := make([]byte, diffChunkSize)

	var extents []SparseExtent
	var totalBytes int64

	off := int64(0)
	for off < size {
		if err := ctx.Err(); err != nil {
			return nil, totalBytes, err
		}
		dataStart, err := unix.Seek(dfd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break
			}
			return nil, totalBytes, fmt.Errorf("SEEK_DATA on delta: %w", err)
		}
		dataEnd, err := unix.Seek(dfd, dataStart, unix.SEEK_HOLE)
		if err != nil {
			return nil, totalBytes, fmt.Errorf("SEEK_HOLE on delta: %w", err)
		}

		extLen := dataEnd - dataStart
		extents = append(extents, SparseExtent{Offset: dataStart, Length: extLen})
		totalBytes += extLen

		for pos := dataStart; pos < dataEnd; {
			if err := ctx.Err(); err != nil {
				return nil, totalBytes, err
			}
			n := int64(len(buf))
			if rem := dataEnd - pos; rem < n {
				n = rem
			}
			if err := preadFull(dfd, buf[:n], pos); err != nil {
				return nil, totalBytes, err
			}
			if err := pwriteFull(tfd, buf[:n], pos); err != nil {
				return nil, totalBytes, err
			}
			pos += n
		}
		off = dataEnd
	}
	return extents, totalBytes, nil
}

// RestoreSparseRegions restores the specified byte extents in dstFile from
// srcFile (modifying dstFile in place). Any extent region where srcFile has a
// hole is punched back to a hole in dstFile.
func RestoreSparseRegions(ctx context.Context, srcFile, dstFile string, extents []SparseExtent) error {
	if len(extents) == 0 {
		return nil
	}
	sf, err := os.Open(srcFile)
	if err != nil {
		return fmt.Errorf("open src %s: %w", srcFile, err)
	}
	defer sf.Close()

	df, err := os.OpenFile(dstFile, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open dst %s: %w", dstFile, err)
	}
	defer df.Close()

	sfd := int(sf.Fd())
	dfd := int(df.Fd())
	buf := make([]byte, diffChunkSize)

	for _, ext := range extents {
		if err := ctx.Err(); err != nil {
			return err
		}
		extEnd := ext.Offset + ext.Length
		pos := ext.Offset
		for pos < extEnd {
			dataStart, serr := unix.Seek(sfd, pos, unix.SEEK_DATA)
			if serr != nil {
				if errors.Is(serr, unix.ENXIO) {
					holeLen := extEnd - pos
					if holeLen > 0 {
						if err := punchOrZero(dfd, buf, pos, holeLen); err != nil {
							return err
						}
					}
					break
				}
				return fmt.Errorf("SEEK_DATA on src: %w", serr)
			}
			if dataStart >= extEnd {
				holeLen := extEnd - pos
				if holeLen > 0 {
					if err := punchOrZero(dfd, buf, pos, holeLen); err != nil {
						return err
					}
				}
				break
			}
			if dataStart > pos {
				holeLen := dataStart - pos
				if err := punchOrZero(dfd, buf, pos, holeLen); err != nil {
					return err
				}
				pos = dataStart
			}
			dataEnd, serr := unix.Seek(sfd, dataStart, unix.SEEK_HOLE)
			if serr != nil {
				return fmt.Errorf("SEEK_HOLE on src: %w", serr)
			}
			if dataEnd > extEnd {
				dataEnd = extEnd
			}
			for pos < dataEnd {
				n := int64(len(buf))
				if rem := dataEnd - pos; rem < n {
					n = rem
				}
				if err := preadFull(sfd, buf[:n], pos); err != nil {
					return err
				}
				if err := pwriteFull(dfd, buf[:n], pos); err != nil {
					return err
				}
				pos += n
			}
		}
	}
	return nil
}

func punchOrZero(fd int, scratch []byte, off, length int64) error {
	err := unix.Fallocate(fd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, length)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EOPNOTSUPP) && !errors.Is(err, syscall.ENOSYS) {
		return fmt.Errorf("fallocate punch hole at %d len %d: %w", off, length, err)
	}
	clear(scratch)
	for pos := off; pos < off+length; {
		n := int64(len(scratch))
		if rem := (off + length) - pos; rem < n {
			n = rem
		}
		if err := pwriteFull(fd, scratch[:n], pos); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

func preadFull(fd int, buf []byte, off int64) error {
	for len(buf) > 0 {
		n, err := unix.Pread(fd, buf, off)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		buf = buf[n:]
		off += int64(n)
	}
	return nil
}

func pwriteFull(fd int, buf []byte, off int64) error {
	for len(buf) > 0 {
		n, err := unix.Pwrite(fd, buf, off)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
		off += int64(n)
	}
	return nil
}

