package future

type StatusType int

const (
	StatusTypeSuccess StatusType = 1
	StatusTypeError   StatusType = 2
)

type Status struct {
	Type StatusType
	Err  error
}

func OKStatus() Status {
	return Status{
		Type: StatusTypeSuccess,
		Err:  nil,
	}
}

func ErrorStatus(err error) Status {
	return Status{
		Type: StatusTypeError,
		Err:  err,
	}
}

func (s Status) IsOk() bool {
	return s.Type == StatusTypeSuccess
}

func (s Status) IsError() bool {
	return s.Type == StatusTypeError
}
