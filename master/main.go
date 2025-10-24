package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/deven96/whatsticker/master/handler"
	"github.com/deven96/whatsticker/master/task"
	"github.com/deven96/whatsticker/master/whatsapp"
	"github.com/deven96/whatsticker/utils"

	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

var ch *amqp.Channel
var convertQueue *amqp.Queue
var loggingQueue *amqp.Queue

type incomingMessageHandler struct{}

func (i *incomingMessageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		verifyToken := r.URL.Query().Get("hub.verify_token")
		mode := r.URL.Query().Get("hub.mode")
		challenge := r.URL.Query().Get("hub.challenge")
		if verifyToken == os.Getenv("VERIFY_TOKEN") && mode == "subscribe" {
			log.Infof("Webhook verification succeeded")
			w.Write([]byte(challenge))
		} else {
			log.Warnf("Webhook verification failed: token=%s, expected=%s, mode=%s", verifyToken, os.Getenv("VERIFY_TOKEN"), mode)
			http.Error(w, "Could not verify challenge", http.StatusBadRequest)
		}
		return
	}

	parsed, err := whatsapp.UnmarshalIncomingMessage(r)
	if err != nil {
		log.Errorf("Failed to parse incoming message: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Infof("Received message: %+v", parsed)
	handler.Run(parsed, ch, convertQueue, loggingQueue)
}

func liveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"schemaVersion": 1,"label": "whatsticker","message": "alive","color": "green"}`))
}

func checkEnv(varName string) string {
	val := os.Getenv(varName)
	if val == "" {
		log.Warnf("Environment variable %s is not defined!", varName)
	}
	return val
}

func main() {
	masterDir, _ := filepath.Abs("./master")
	logLevel := flag.String("log-level", "INFO", "Set log level to one of (INFO/DEBUG)")
	port := flag.String("port", "9000", "Set port to start incoming streaming server")
	flag.Parse()

	if ll := os.Getenv("LOG_LEVEL"); ll != "" {
		llg := strings.ToUpper(ll)
		logLevel = &llg
	}
	log.SetLevel(utils.GetLogLevel(*logLevel))

	fmt.Println("Master directory:", masterDir)
	log.Infof("Log level set to %s", *logLevel)

	// Checando variáveis críticas
	verifyToken := checkEnv("VERIFY_TOKEN")
	bearerToken := checkEnv("BEARER_ACCESS_TOKEN")
	convertQueueName := checkEnv("CONVERT_TO_WEBP_QUEUE")
	sendWebpQueueName := checkEnv("SEND_WEBP_TO_WHATSAPP_QUEUE")
	logMetricQueueName := checkEnv("LOG_METRIC_QUEUE")

	if verifyToken == "" || bearerToken == "" || convertQueueName == "" || sendWebpQueueName == "" || logMetricQueueName == "" {
		log.Fatal("Algumas variáveis de ambiente essenciais não estão definidas. Corrija antes de continuar.")
	}

	// Conectando ao RabbitMQ
	amqpConfig := utils.GetAMQPConfig()
	conn, err := amqp.Dial(amqpConfig.Uri)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	log.Info("Connected to RabbitMQ")

	ch, err = conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()
	log.Info("Opened RabbitMQ channel")

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}
	log.Info("QoS set to 1")

	convertQueue = utils.GetQueue(ch, convertQueueName, true)
	completeQueue := utils.GetQueue(ch, sendWebpQueueName, true)
	loggingQueue = utils.GetQueue(ch, logMetricQueueName, false)

	log.Infof("Queues configured: convert=%s, complete=%s, logging=%s", convertQueue.Name, completeQueue.Name, loggingQueue.Name)

	complete := &task.StickerConsumer{PushMetricsTo: loggingQueue}
	completeQueueMsgs, err := ch.Consume(
		completeQueue.Name, "", false, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	go func() {
		for d := range completeQueueMsgs {
			log.Infof("Processing message from queue: %s", d.MessageId)
			complete.Execute(ch, &d)
		}
	}()

	// Servidores HTTP
	http.Handle("/incoming", &incomingMessageHandler{})
	http.Handle("/webhook", &incomingMessageHandler{})
	http.Handle("/", http.HandlerFunc(liveness))

	log.Infof("Starting server on port %s...", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatalf("Could not start server on %s: %v", *port, err)
	} else {
		log.Infof("Server started on %s", *port)
	}
}