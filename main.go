package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/diapco/votecube-crud/deserialize"
	"github.com/diapco/votecube-crud/models"
	_ "github.com/lib/pq"
	"github.com/valyala/fasthttp"
	"github.com/volatiletech/sqlboiler/queries/qm"
	"log"
)

var (
	addr     = flag.String("addr", ":10100", "TCP address to listen to")
	compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
	proc     RequestProcessor
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
		request := deserialize.CreatePollRequest{
			Ctx:  ctx,
			Done: make(chan bool),
		}

		proc.batch.Add <- &request

		<-request.Done
		//ctx.PostBody()
		//var json = jsoniter.ConfigCompatibleWithStandardLibrary

		//var data models.Direction

		//json.Unmarshal(ctx.PostBody(), &data)

		//rawData := ctx.PostBody()

		//cursorPosition := 2

		//var cursor *int = &cursorPosition

		//data.DirectionDescription = "hello"

		//seqBlocks, err := directionId.GetBlocks(9)
		//
		//if err != nil {
		//	fmt.Fprintf(ctx, "Error")
		//}
		//
		//for _, seqBlock := range seqBlocks {
		//	fmt.Fprintf(ctx, "Block, Start: %v, Length: %v\n", seqBlock.Start, seqBlock.Length)
		//}
		/*		fmt.Fprintf(ctx, "Hello, world!\n\n")

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

				fmt.Fprintf(ctx, "Raw request is:\n---CUT---\n%s\n---CUT---", &ctx.Request)*/
	}

	/*	ctx.SetContentType("text/plain; charset=utf8")

		// Set arbitrary headers
		ctx.Response.Header.Set("Access-Control-Allow-Methods", "PUT")
		ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")

		// Set cookies
		var c fasthttp.Cookie
		c.SetKey("cookie-name")
		c.SetValue("cookie-value")
		ctx.Response.Header.SetCookie(&c)*/
}

func main() {
	flag.Parse()

	db, err := sql.Open("postgres", `postgresql://root@localhost:26257/votecube?sslmode=disable`)
	if err != nil {
		panic(err)
	}
	numInitialDbRequests := 5
	initialDbRequestsDone := make(chan bool, numInitialDbRequests)
	locationMaps := deserialize.LocationMaps{}
	var theThemeMap *map[int64]models.Theme

	go getContinents(&locationMaps, db, initialDbRequestsDone)
	go getCountries(&locationMaps, db, initialDbRequestsDone)
	go getStates(&locationMaps, db, initialDbRequestsDone)
	go getTowns(&locationMaps, db, initialDbRequestsDone)
	go getThemes(theThemeMap, db, initialDbRequestsDone)

	numCompletedInitialDbRequests := 0
	for range initialDbRequestsDone {
		numCompletedInitialDbRequests++
		if numCompletedInitialDbRequests == numInitialDbRequests {
			break
		}
	}
	proc.startProcessing(&locationMaps, theThemeMap)

	//db = SetupDb()

	h := requestHandler
	//if *compress {
	//	h = fasthttp.CompressHandler(h)
	//}

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func getContinents(maps *deserialize.LocationMaps, db *sql.DB, done chan bool) {
	continents, err := models.Continents(
		qm.Select(models.ContinentColumns.ContinentID),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Continents")
		panic(err)
	}

	continentMap := make(map[int64]models.Continent)

	for _, continent := range continents {
		continentMap[continent.ContinentID] = *continent
	}

	maps.ContinentMap = continentMap

	done <- true
}

func getCountries(maps *deserialize.LocationMaps, db *sql.DB, done chan bool) {
	countries, err := models.Countries(
		qm.Select(models.CountryColumns.CountryID, models.CountryColumns.ContinentID),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Countries")
		panic(err)
	}

	countryMap := make(map[int64]models.Country)

	for _, country := range countries {
		countryMap[country.ContinentID] = *country
	}

	maps.CountryMap = countryMap

	done <- true
}

func getStates(maps *deserialize.LocationMaps, db *sql.DB, done chan bool) {
	states, err := models.States(
		qm.Select(models.StateColumns.StateID, models.StateColumns.CountryID),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying States")
		panic(err)
	}

	stateMap := make(map[int64]models.State)

	for _, state := range states {
		stateMap[state.StateID] = *state
	}

	maps.StateMap = stateMap

	done <- true
}

func getTowns(maps *deserialize.LocationMaps, db *sql.DB, done chan bool) {
	towns, err := models.Towns(
		qm.Select(models.TownColumns.TownID, models.TownColumns.StateID),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Towns")
		panic(err)
	}

	townMap := make(map[int64]models.Town)

	for _, town := range towns {
		townMap[town.TownID] = *town
	}

	maps.TownMap = townMap

	done <- true
}

func getThemes(theThemeMap *map[int64]models.Theme, db *sql.DB, done chan bool) {
	themes, err := models.Themes(
		qm.Select(models.ThemeColumns.ThemeID),
	).All(context.Background(), db)

	if err != nil {
		fmt.Errorf("error querying Towns")
		panic(err)
	}

	themeMap := make(map[int64]models.Theme)

	for _, theme := range themes {
		themeMap[theme.ThemeID] = *theme
	}

	theThemeMap = &themeMap

	done <- true
}
