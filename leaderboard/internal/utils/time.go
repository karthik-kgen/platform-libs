package utils

import (
	"time"
)

func GetCurrTimeStamp() time.Time {
	return time.Now().UTC()
}
