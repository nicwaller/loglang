package loglang

func Coalesce(args ...any) any {
	for _, v := range args {
		if v != nil {
			switch v.(type) {
			case string:
				if v.(string) != "" {
					return v
				}
			default:
				return v
			}
			return v
		}
	}
	return nil
}
