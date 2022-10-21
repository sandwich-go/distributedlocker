package distributedlocker

import "time"

const (
	minExpiration    = 8 * time.Second
	minRetryInterval = 50 * time.Millisecond
	maxRetryInterval = 250 * time.Millisecond
)

//go:generate optionGen  --option_return_previous=false --usage_tag_name=usage
func OptionsOptionDeclareWithDefault() interface{} {
	return map[string]interface{}{
		"RetryTimes ":      int(32),                         // @MethodComment(acquire lock重试次数)
		"MinRetryInterval": time.Duration(minRetryInterval), // @MethodComment(最小重试时间，不能小于50ms)
		"MaxRetryInterval": time.Duration(maxRetryInterval), // @MethodComment(最大重试时间，不能大于250ms)
		"DriftFactor":      float64(0.01),                   // @MethodComment(有效时间因子)
		"TimeoutFactor":    float64(0.05),                   // @MethodComment(超时时间因子)
		"Expiration":       time.Duration(minExpiration),    // @MethodComment(过期时间)
		"Prefix":           "__mx__",                        // @MethodComment(key的前缀)
		"AutoRenew":        true,                            // @MethodComment(是否自动续期)
	}
}
