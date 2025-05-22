package future

type Future struct {
	response chan struct{}
	isDone   bool
	status   Status
}

func NewFuture() *Future {
	return &Future{
		response: make(chan struct{}),
		isDone:   false,
		status:   OKStatus(),
	}
}

func (f *Future) markDone() {
	if !f.isDone {
		f.isDone = true
		close(f.response)
	}
}

func (f *Future) Status() Status {
	return f.status
}

func (f *Future) Wait() {
	<-f.response
}

func (f *Future) MarkDoneAsError(err error) {
	f.markDone()
	f.status = ErrorStatus(err)
}

func (f *Future) MarkDoneAsOk() {
	f.markDone()
	f.status = OKStatus()
}
