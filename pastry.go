package pastry

import (
	"fmt"
)

// TimeoutError represents an error that was raised when a call has taken too long. It is its own type for the purposes of handling the error.
type TimeoutError struct {
	Action  string
	Timeout int
}

// Error returns the TimeoutError as a string and fulfills the error interface.
func (t TimeoutError) Error() string {
	return fmt.Sprintf("Timeout error: %s took more than %v seconds.", t.Action, t.Timeout)
}

// throwTimeout creates a new TimeoutError from the action and timeout specified.
func throwTimeout(action string, timeout int) TimeoutError {
	return TimeoutError{
		Action:  action,
		Timeout: timeout,
	}
}

type reqMode int

const mode_set = reqMode(0)
const mode_get = reqMode(1)
const mode_del = reqMode(2)
const mode_prx = reqMode(3)
const mode_scan = reqMode(4)
