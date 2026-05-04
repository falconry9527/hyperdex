package msg

// HTTP-level and business-level response codes. The set is intentionally
// trimmed to what the api service actually returns; mirror seabond-api's
// const.go shape so frontends can share decoding logic.
const (
	CodeSuccess = 200 // success

	CodeError         = 400 // generic client error
	NotFind           = 401 // record not found
	CodeInternalError = 500 // internal server error
	ParamError        = 504 // invalid parameter
)

var codeMsg = map[int]string{
	CodeSuccess:       "Success",
	CodeError:         "Error",
	NotFind:           "Record not found",
	CodeInternalError: "Internal server error",
	ParamError:        "Invalid parameter",
}

// GetMsg returns the human-readable message for the given response code.
func GetMsg(code int) string {
	if m, ok := codeMsg[code]; ok {
		return m
	}
	return codeMsg[CodeInternalError]
}
