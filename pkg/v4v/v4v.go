package v4v

import (
	"bytes"
	"encoding/binary"
	"io"
	"syscall"
)

func (v *V4VAddr) toC(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, v.Port)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, v.Domain)
	if err != nil {
		return err
	}

	return nil
}

func (v *V4VRingId) toC(w io.Writer) error {
	err := v.Addr.toC(w)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, v.Partner)
	if err != nil {
		return err
	}

	return nil
}

func Open(sockType int) (*V4VSocket, error) {
	switch sockType {
	case SOCK_STREAM:
		f, err := os.OpenFile("/dev/v4v_stream", syscall.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
	case SOCK_DGRAM:
		f, err := os.OpenFile("/dev/v4v_dgram", syscall.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
	default:
		return nil, error.New("unsupported socket type")
	}

	return &V4VSocket{fd: f}, nil
}

func (s *V4VSocket) Close() error {
	return s.fd.Close()
}

func (s *V4VSocket) Bind(addr V4VAddr, partner DomainId) error {
	var buf bytes.Buffer

	id := V4VRingId {addr: addr, partner: partner}

	if addr.domain == 0 {
		addr->domain = V4V_DOMID_ANY;
	}

	V4VRingId.toC(buf)

	 _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(s.fd.Fd()),
		uintptr(V4VIOCBIND),
		uintptr(unsafe.Pointer(&buf)),
	)

	if errno != 0 {
		return  errors.New(errno.Error())
	}

	return nil
}

func (s *V4VSocket) Connect(addr V4VAddr) error {
	var buf bytes.Buffer

	addr.toC(buf)

	 _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(s.fd.Fd()),
		uintptr(V4VIOCCONNECT),
		uintptr(unsafe.Pointer(&buf)),
	)

	if errno != 0 {
		return  errors.New(errno.Error())
	}

	return nil
}

func (s *V4VSocket) Listen(backlog int) error {
	var buf bytes.Buffer

	 _, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(s.fd.Fd()),
		uintptr(V4VIOCLISTEN),
		uintptr(&backlog),
	)

	if errno != 0 {
		return  errors.New(errno.Error())
	}

	return nil
}

func (s *V4VSocket) Accept(addr V4VAddr) (*V4VSocket, error) {
	var buf bytes.Buffer

	addr.toC(buf)

	 fd, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(s.fd.Fd()),
		uintptr(V4VIOACCEPT),
		uintptr(unsafe.Pointer(&buf)),
	)

	if errno != 0 {
		return  -1, errors.New(errno.Error())
	}

	return &V4VSocket{fd: os.NewFile(fd,"")}, nil
}

func (s *V4VSocket) Send(msg []byte, size SizeT, flags int) (int, error) {
	var op V4VDev;

	op.Buf = msg[:]
	op.Len = size
	op.Flags = flags
	op.Addr = 0;

	 b, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(s.fd.Fd()),
		uintptr(V4VIOCLISTEN),
		uintptr(&backlog),
	)

	if errno != 0 {
		return  errors.New(errno.Error())
	}

	return int(b), nil
}
