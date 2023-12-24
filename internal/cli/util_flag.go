package cli

import (
	"strconv"
	"strings"
)

type flagSet struct {
	path    string
	rawArgs []string
	args    map[string]flagValue
}

func flagParse(s string) flagSet {

	s = strings.TrimSpace(s)

	fset := flagSet{
		path:    "",
		rawArgs: strings.Split(s, " "),
		args:    map[string]flagValue{},
	}

	if n := strings.Index(s, " -"); n >= 0 {
		fset.path = s[:n]
	} else {
		fset.path = s
	}

	for i, k := range fset.rawArgs {

		if len(k) < 2 || k[0] != '-' {
			continue
		}

		k = strings.Trim(k, "-")

		if n := strings.Index(k, "="); n > 0 {
			if n+1 < len(k) {
				fset.args[k[:n]] = flagValue(k[n+1:])
			} else {
				fset.args[k[:n]] = flagValue("")
			}
			continue
		}

		if len(fset.rawArgs) <= (i+1) || fset.rawArgs[i+1][0] == '-' {
			fset.args[k] = flagValue([]byte(""))
			continue
		}

		v := fset.rawArgs[i+1]

		fset.args[k] = flagValue([]byte(v))
	}

	return fset
}

func (it *flagSet) ValueOK(key string) (flagValue, bool) {

	if v, ok := it.args[key]; ok {
		return v, ok
	}

	return nil, false
}

func (it *flagSet) Value(key string) flagValue {

	if v, ok := it.ValueOK(key); ok {
		return v
	}

	return flagValue{}
}

func (it *flagSet) Each(fn func(key, val string)) {
	for k, v := range it.args {
		fn(k, v.String())
	}
}

// Universal Bytes
type flagValue []byte

// String converts the value-bytes to string
func (bx flagValue) String() string {
	return string(bx)
}

// Bool converts the value-bytes to bool
func (bx flagValue) Bool() bool {
	if len(bx) > 0 {
		if b, err := strconv.ParseBool(string(bx)); err == nil {
			return b
		}
	}
	return false
}

// Int64 converts the value-bytes to int64
func (bx flagValue) Int64() int64 {
	if len(bx) > 0 {
		if i64, err := strconv.ParseInt(string(bx), 10, 64); err == nil {
			return i64
		}
	}
	return 0
}

// Float64 converts the value-bytes to float64
func (bx flagValue) Float64() float64 {
	if len(bx) > 0 {
		if f64, err := strconv.ParseFloat(string(bx), 64); err == nil {
			return f64
		}
	}
	return 0

}
