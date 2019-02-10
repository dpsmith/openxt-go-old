package v4v

type SizeT uint16
type DomainID uint16

type V4VAddr struct {
	Port uint32
	Domain DomainId
}

type V4VRingId struct {
	Addr V4VAddr
	Partner DomainId
}

type V4VDev struct {
	Buf []byte
	Len SizeT
	Flags int
	Addr uint64
}

type V4VSocket struct {
	fd *os.File
}
