package dbd

import (
	"github.com/godbus/dbus"
	"github.com/godbus/dbus/introspect"
)

const (
	dbdIntrospectString = `
<interface name="com.citrix.xenclient.db">
    <method name="read">
      <arg name="path" type="s" direction="in"/>
      <arg name="value" type="s" direction="out"/>
    </method>
    <method name="read_binary">
      <arg name="path" type="s" direction="in"/>
      <arg name="value" type="ay" direction="out"/>
    </method>
    <method name="write">
      <arg name="path" type="s" direction="in"/>
      <arg name="value" type="s" direction="in"/>
    </method>
    <method name="dump">
      <arg name="path" type="s" direction="in"/>
      <arg name="value" type="s" direction="out"/>
    </method>
    <method name="inject">
      <arg name="path" type="s" direction="in"/>
      <arg name="value" type="s" direction="in"/>
    </method>
    <method name="list">
      <arg name="path" type="s" direction="in"/>
      <arg name="value" type="as" direction="out"/>
    </method>
    <method name="rm">
      <arg name="path" type="s" direction="in"/>
    </method>
    <method name="exists">
      <arg name="path" type="s" direction="in"/>
      <arg name="ex" type="b" direction="out"/>
    </method>
</interface>
`
	dbdInterface = "com.citrix.xenclient.db"
)

type Server struct {
	db *Database
}

func NewServer(config string) (Server, error) {
	methodmapping := map[string]string {
		"Read" : "read",
		"ReadBinary" : "read_binary",
		"Write" : "write",
		"Dump" : "dump",
		"Inject" : "inject",
		"List" : "list",
		"Rm" : "rm",
		"Exists" : "exists",
	}

	s := &Server{}
	s.db = &Database{}
	err := s.db.Open(config)
	if err != nil {
		return nil, err
	}

	conn, err := dbus.SystemBus()
	if err != nil {
		panic(err)
	}

	reply, err := conn.RequestName(dbdInterface, dbus.NameFlagDoNotQueue)
	if err != nil {
		return nil, err
	}
	if reply != dbus.RequestNameReplyPrimaryOwner {
		errors.New("name (%s) already taken", dbdInterface)
	}

	err := conn.ExportWithMap(s, methodmapping, "/", dbdInterface)
	if err != nil {
		return nil, err
	}

	intro := "<node>" + dbdIntrospectString + introspect.IntrospectDataString + "</node>"

	err := conn.Export(introspect.Introspectable(intro), "/",
		"org.freedesktop.DBus.Introspectable")
	if err != nil {
		return nil, err
	}
}

func (s *Server) Read(path string) (string, *dbus.Error) {
	resp, err := s.db.Read(path)
	return resp, err
}

func (s *Server) ReadBinary(path string) ([]byte, *dbus.Error) {
	resp, err := s.db.ReadBinary(path)
	return resp, err
}

func (s *Server) Write(path string, value string) *dbus.Error {
	err := s.db.Write(path, value)
	return err
}

func (s *Server) Dump(path string) (string, *dbus.Error) {
	resp, err := s.db.Dump(path)
	return resp, err
}

func (s *Server) Inject(path string, value string) *dbus.Error {
	err := s.db.Inject(path)
	return err
}

func (s *Server) List(path string) ([]string, *dbus.Error) {
	resp, err := s.db.List(path)
	return resp, err
}

func (s *Server) Rm(path string) *dbus.Error {
	err := s.db.Rm(path)
	return err
}

func (s *Server) Exists(path string) (bool, *dbus.Error) {
	resp, err := s.db.Exists(path)
	return resp, err
}
