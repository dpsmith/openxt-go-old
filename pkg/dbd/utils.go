package dbd

import (
	"io/ioutil"
	"encoding/json"
	"errors"
	"strings"
)

func fcopy(src string, dst string) (int, error) {
    data, err := ioutil.ReadFile(src)
    if err != nil {
	    return 0, err
    }
    if !json.Valid(data) {
	    return 0, errors.New("invalid json file: %s", src)
    }
    err = ioutil.WriteFile(dst, data, 0640)
    if err != nil {
	    return 0, err
    }

    return len(data), nil
}

func validPath(path string) bool {
	return !strings.ContainsAny(path, ". # *")
}

func splitPath(path string) (string string) {
	var ns, path string

	split := strings.splitN(strings.TrimLeft(path, "/"), "/", 2)

	if len(split) == 2 {
		ns = split[0]
		path = split[1]
	} else {
		ns = path
		path = ""
	}

	return ns, path
}

func convertPath(path string) string {
	return strings.Replace(path, "/", ".", -1)
}
