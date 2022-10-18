package distributedlocker

import "time"

const minExpiration = 5 * time.Second

//go:generate optionGen  --option_return_previous=false
func OptionsOptionDeclareWithDefault() interface{} {
	return map[string]interface{}{
		"RetryTimes ":   int(5),
		"RetryInterval": time.Duration(time.Duration(50) * time.Millisecond),
		"Expiration":    time.Duration(minExpiration),
		"Prefix":        "__mx__",
	}
}
