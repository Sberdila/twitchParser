package main

import (
	"net/http"
	"io/ioutil"
	"fmt"
	"regexp"
	"encoding/json"
	"strconv"
	"container/list"
	"sync"
	"time"
	"log"
	"os"
	"strings"
	"math"
)
type MessagesContainer struct {
	Data []struct {
		Type string `json:"type"`
		ID string `json:"id"`
		Attributes struct {
			Command string `json:"command"`
			Room string `json:"room"`
			Timestamp int64 `json:"timestamp"`
			VideoOffset int `json:"video-offset"`
			Deleted bool `json:"deleted"`
			Message string `json:"message"`
			From string `json:"from"`
			Tags struct {
				Badges interface{} `json:"badges"`
				Color interface{} `json:"color"`
				DisplayName string `json:"display-name"`
				Emotes interface{} `json:"emotes"`
				ID string `json:"id"`
				Mod bool `json:"mod"`
				RoomID string `json:"room-id"`
				SentTs string `json:"sent-ts"`
				Subscriber bool `json:"subscriber"`
				TmiSentTs string `json:"tmi-sent-ts"`
				Turbo bool `json:"turbo"`
				UserID string `json:"user-id"`
				UserType interface{} `json:"user-type"`
			} `json:"tags"`
			Color interface{} `json:"color"`
		} `json:"attributes"`
		Links struct {
			Self string `json:"self"`
		} `json:"links"`
	} `json:"data"`
	Meta struct {
		Next interface{} `json:"next"`
	} `json:"meta"`
}


var words = [173]string{"a",
	"about",
	"above",
	"after",
	"again",
	"against",
	"all",
	"am",
	"an",
	"and",
	"any",
	"are",
	"aren't",
	"as",
	"at",
	"be",
	"because",
	"been",
	"before",
	"being",
	"below",
	"between",
	"both",
	"but",
	"by",
	"can't",
	"cannot",
	"could",
	"couldn't",
	"did",
	"didn't",
	"do",
	"does",
	"doesn't",
	"doing",
	"don't",
	"down",
	"during",
	"each",
	"few",
	"for",
	"from",
	"further",
	"had",
	"hadn't",
	"has",
	"hasn't",
	"have",
	"haven't",
	"having",
	"he",
	"he'd",
	"he'll",
	"he's",
	"her",
	"here",
	"here's",
	"hers",
	"herself",
	"him",
	"himself",
	"his",
	"how",
	"how's",
	"i",
	"i'd",
	"i'll",
	"i'm",
	"i've",
	"if",
	"in",
	"into",
	"is",
	"isn't",
	"it",
	"it's",
	"its",
	"itself",
	"let's",
	"me",
	"more",
	"most",
	"mustn't",
	"my",
	"myself",
	"no",
	"nor",
	"not",
	"of",
	"off",
	"on",
	"once",
	"only",
	"or",
	"other",
	"ought",
	"our",
	"ours	ourselves",
	"out",
	"over",
	"own",
	"same",
	"shan't",
	"she",
	"she'd",
	"she'll",
	"she's",
	"should",
	"shouldn't",
	"so",
	"some",
	"such",
	"than",
	"that",
	"that's",
	"the",
	"their",
	"theirs",
	"them",
	"themselves",
	"then",
	"there",
	"there's",
	"these",
	"they",
	"they'd",
	"they'll",
	"they're",
	"they've",
	"this",
	"those",
	"through",
	"to",
	"too",
	"under",
	"until",
	"up",
	"very",
	"was",
	"wasn't",
	"we",
	"we'd",
	"we'll",
	"we're",
	"we've",
	"were",
	"weren't",
	"what",
	"what's",
	"when",
	"when's",
	"where",
	"where's",
	"which",
	"while",
	"who",
	"who's",
	"whom",
	"why",
	"why's",
	"with",
	"won't",
	"would",
	"wouldn't",
	"you",
	"you'd",
	"you'll",
	"you're",
	"you've",
	"your",
	"yours",
	"yourself",
	"yourselves"}

var mutex = &sync.Mutex{}
var MessagesContainerArray  = list.New()
var FailedRequests  = list.New()
var Messages map[int64][]string
var requests = 0
var Offsets map[int64]int
var videoId = "v141999218"
func main() {

	var start, end int
	var buffer []string
	var wg sync.WaitGroup
	var sum = 0
	var goroutines = 100;
	values := list.New()
	var index = 0
	var counter = 0


	buffer = getTimeData(videoId)
	start, _  =  strconv.Atoi(buffer[2])
	end, _ = strconv.Atoi(buffer[3])
	Messages = make(map[int64][]string)
	Offsets = make(map[int64]int)
	println(start,end)
	number := end-start
	//fmt.Println(start)
	//fmt.Println(end)
	//fmt.Println(number)

	var bufferRoutines = goroutines
	for number > 0 && goroutines > 0 {
		var a = math.Floor(float64 (number/goroutines/50)) * 50;
		number = number - int(a)
		goroutines--
		values.PushFront(a)
		index ++
	}

	var slice = make([]int,index)
	for e := values.Front(); e != nil; e = e.Next() {
		slice[counter] = int( e.Value.(float64))
		counter++
	}

	goroutines = bufferRoutines
	for i:=0 ; i < goroutines; i++ {
		wg.Add(1)
		go process(start+sum,start+sum+slice[i],&wg)

		sum=sum+slice[i]

	}

	wg.Wait()

	if(FailedRequests.Len()>0) {
		for e := FailedRequests.Front(); e != nil; e = e.Next() {
			wg.Add(1)
			Singleprocess(e.Value.(int),&wg)
		}

	}
	wg.Wait()



	/*for index,element := range Messages {
		fmt.Println(index,",",strings.Join(element,""))
	}
	*/
	writeToFile()
}

func process(start int,end int, wg *sync.WaitGroup) {

	var chatArray MessagesContainer
	for i := start; i < end  ; i++ {
	fmt.Println("looping")
		parseJson(getChatData(start,videoId),&chatArray)
		MessagesContainerArray.PushFront(chatArray)
		start++
		for j:=0;j<len(chatArray.Data) ; j++ {
			mutex.Lock()
			Messages[chatArray.Data[j].Attributes.Timestamp]= append(Messages[chatArray.Data[j].Attributes.Timestamp], chatArray.Data[j].Attributes.Message)

			value, ok := Offsets[chatArray.Data[j].Attributes.Timestamp]
			if ok {
				fmt.Println(value)
			} else {
				Offsets[chatArray.Data[j].Attributes.Timestamp] = chatArray.Data[j].Attributes.VideoOffset

			}
			mutex.Unlock()
		}

	}
	fmt.Println("this is done")

	wg.Done()


}

func Singleprocess(start int, wg *sync.WaitGroup) {

	var chatArray MessagesContainer
	fmt.Println("going for ",start," redoing failed request  ")

		parseJson(getChatData(start,"v138828567"),&chatArray)
		MessagesContainerArray.PushFront(chatArray)
		start++
		for j:=0;j<len(chatArray.Data) ; j++ {
			mutex.Lock()
			Messages[chatArray.Data[j].Attributes.Timestamp]= append(Messages[chatArray.Data[j].Attributes.Timestamp], chatArray.Data[j].Attributes.Message)
			//fmt.Printf("%T: %+v\n", Messages, Messages)
			mutex.Unlock()

		}



	wg.Done()


}

func getTimeData (videoId string) []string {

	var bufferArray  []string
	re := regexp.MustCompile("[0-9]+")
	bufferArray = re.FindAllString(getJsonString("https://rechat.twitch.tv/rechat-messages?start=1458961896&amp;video_id="+videoId),-1)

	return bufferArray

}

func getChatData(timestamp int,videoId string) [] byte {

	var url string
	var timestampString string
	 timestampString = strconv.Itoa(timestamp)
	url = "https://rechat.twitch.tv/rechat-messages?start="
	url = url + timestampString + "&amp;video_id=" + videoId
	return getJson(url,timestamp)

}



func getJson (url string,timestamp int) [] byte {

	var c = &http.Client{
		Timeout: 15 * time.Second,
	}
	res, err := c.Get(url)
	if err != nil {
		FailedRequests.PushFront(timestamp)
		fmt.Println("Failed",timestamp)
		return nil
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err.Error())
	}
	requests++
	//fmt.Println(requests)
	defer timeTrack(time.Now(), "request")

	return body
}


func getJsonString (url string) string {

	res, err := http.Get(url)
	if err != nil {
		panic(err.Error())
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err.Error())
	}

	return string(body)
}

func parseJson(data []byte, container * MessagesContainer) {

	err1 := json.Unmarshal(data, &container)
	if err1 != nil {
		fmt.Println("error:", err1)
	}
	//fmt.Printf("%T: %+v\n", container, container)
}
func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)

	log.Printf("%s took %s", name, elapsed)
	if(int64(time.Since(start).Minutes())>0) {
		log.Printf("Requests per minute %d ",int64(requests)/int64(time.Since(start).Minutes()) )

	}


}

func writeToFile(){

	var stringToWrite string
	var indexBuffer string
	var buffer string
	f, err := os.Create(videoId+".csv")
	check(err)
	defer f.Close()
	for index,element := range Messages {
		buffer = strings.Join(element,"")
		indexBuffer = strconv.FormatInt(index,10)
		offsetBuffer := strconv.Itoa(Offsets[index])
		stringToWrite =  "\n" + indexBuffer   + " ; " + strings.Replace(strings.Replace(buffer," ; "," ",-1),","," ",-1)  + " ;" + offsetBuffer

		f.Write([]byte(removeStopWords(stringToWrite)))
		//fmt.Println("Wrote this =>",stringToWrite)
		f.Sync()

	}




}

func removeStopWords(s string) string {
	reStr := ""

	for i, word := range words {
		if i != 0 {
			reStr += `|`
		}
		reStr += `\Q ` + word + ` \E`
	}
	re := regexp.MustCompile(reStr)
	return re.ReplaceAllLiteralString(s,"")
}


func check(e error) {
	if e != nil {
		panic(e)
	}
}