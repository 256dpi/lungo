package lungo

import (
	"fmt"
	"reflect"
)

const (
	supported = "supported"
	ignored   = "ignored"
)

func assertOptions(opts interface{}, fields map[string]string) {
	// get value
	value := reflect.ValueOf(opts).Elem()

	// check fields
	for i := 0; i < value.NumField(); i++ {
		// get name
		name := value.Type().Field(i).Name

		// check if field is supported
		support := fields[name]
		if support == supported || support == ignored {
			continue
		}

		// otherwise assert field is nil
		if !value.Field(i).IsNil() {
			panic(fmt.Sprintf("lungo: unsupported option: %s", name))
		}
	}
}
