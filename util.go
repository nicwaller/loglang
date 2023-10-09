package loglang

func Map[I any, O any](mapper func(I) O, orig []I) []O {
	changed := make([]O, 0, len(orig))
	for _, v := range orig {
		changed = append(changed, mapper(v))
	}
	return changed
}

func CoalesceStr(args ...string) string {
	for _, v := range args {
		if v != "" {
			return v
		}
	}
	return ""
}

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
		}
	}
	return nil
}

type NamedEntity[T any] struct {
	value T
	name  string
}
