package msg

import "github.com/gin-gonic/gin"

// Response is the standard JSON envelope returned by all API handlers.
type Response struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"msg"`
}

// Result writes a JSON response with the given code, data, and message.
func Result(c *gin.Context, code int, data interface{}, msg string) {
	c.JSON(200, Response{Code: code, Data: data, Msg: msg})
}

// Success writes a successful JSON response with the given data payload.
func Success(c *gin.Context, data interface{}) {
	Result(c, CodeSuccess, data, "SUCCESS")
}

// Error writes an error response derived from err.
func Error(c *gin.Context, err error) {
	if err == nil {
		Result(c, CodeError, nil, "UNKNOWN ERROR")
		return
	}
	Result(c, CodeError, nil, err.Error())
}

// ErrorCode writes an error response for the given business code.
func ErrorCode(c *gin.Context, code int) {
	Result(c, code, nil, GetMsg(code))
}
