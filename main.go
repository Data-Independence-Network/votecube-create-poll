package main

import (
	"flag"
	"fmt"
	"github.com/diapco/votecube-crud/models"
	_ "github.com/lib/pq"
	"github.com/valyala/fasthttp"
	"log"
)

var (
	addr     = flag.String("addr", ":10100", "TCP address to listen to")
	compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
	//db          *sql.DB
	//directionId sequence.Sequence
)

/**

Create process (v1 - completely new poll, no batching):

1)	Read in the entire data structure
2)	Create objects in reverse order (deepest dependencies first)
3)  Return new Ids only datastructure that ties in the temporary UI Ids to the created ones

*/

func requestHandler(ctx *fasthttp.RequestCtx) {

	if ctx.IsPut() {
		//var json = jsoniter.ConfigCompatibleWithStandardLibrary

		var data models.Direction

		//json.Unmarshal(ctx.PostBody(), &data)

		//rawData := ctx.PostBody()

		//cursorPosition := 2

		//var cursor *int = &cursorPosition

		data.DirectionDescription = "hello"

		//seqBlocks, err := directionId.GetBlocks(9)
		//
		//if err != nil {
		//	fmt.Fprintf(ctx, "Error")
		//}
		//
		//for _, seqBlock := range seqBlocks {
		//	fmt.Fprintf(ctx, "Block, Start: %v, Length: %v\n", seqBlock.Start, seqBlock.Length)
		//}
	} else {
		fmt.Fprintf(ctx, "Hello, world!\n\n")

		fmt.Fprintf(ctx, "Request method is %q\n", ctx.Method())

		fmt.Fprintf(ctx, "RequestURI is %q\n", ctx.RequestURI())
		fmt.Fprintf(ctx, "Requested path is %q\n", ctx.Path())
		fmt.Fprintf(ctx, "Host is %q\n", ctx.Host())
		fmt.Fprintf(ctx, "Query string is %q\n", ctx.QueryArgs())
		fmt.Fprintf(ctx, "User-Agent is %q\n", ctx.UserAgent())
		fmt.Fprintf(ctx, "Connection has been established at %s\n", ctx.ConnTime())
		fmt.Fprintf(ctx, "Request has been started at %s\n", ctx.Time())
		fmt.Fprintf(ctx, "Serial request number for the current connection is %d\n", ctx.ConnRequestNum())
		fmt.Fprintf(ctx, "Your ip is %q\n\n", ctx.RemoteIP())

		fmt.Fprintf(ctx, "Raw request is:\n---CUT---\n%s\n---CUT---", &ctx.Request)
	}

	ctx.SetContentType("text/plain; charset=utf8")

	// Set arbitrary headers
	ctx.Response.Header.Set("Access-Control-Allow-Methods", "PUT")
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")

	// Set cookies
	var c fasthttp.Cookie
	c.SetKey("cookie-name")
	c.SetValue("cookie-value")
	ctx.Response.Header.SetCookie(&c)
}

func main() {
	flag.Parse()

	//db = SetupDb()

	h := requestHandler
	//if *compress {
	//	h = fasthttp.CompressHandler(h)
	//}

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}
