package dal

import "os"

type Options struct {
	PageSize int

	MinFillPercent float32
	MaxFillPercent float32
}

var DefaultOptions = &Options{
	PageSize:       os.Getpagesize(),
	MinFillPercent: 0.4,
	MaxFillPercent: 0.95,
}
