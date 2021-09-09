package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type PayloadGenerator func(i int, j int64) string

func defaultPayloadGen() PayloadGenerator {
	return func(i int, j int64) string {
		return fmt.Sprintf("this is msg #%d!", i)
	}
}

func litmusPayloadGen() PayloadGenerator {
	return func(i int, j int64) string {
		return fmt.Sprintf(`{"success":true,"datatype":"int32","timestamp":%d,"registerId":"57D9F461-ADDB-4045-8F9A-417AF8C1BBBF","value":3118,"deviceID":"6CB16FD4-A869-4FDE-9282-CFF7D7771DE5","tagName":"Process Variable %d","deviceName":"sim1","description": "" }`, j, i)
	}
}

func constantPayloadGenerator(payload string) PayloadGenerator {
	return func(i int, j int64) string {
		return payload
	}
}

func filePayloadGenerator(filepath string) PayloadGenerator {
	inputPath := strings.Replace(filepath, "@", "", 1)
	content, err := ioutil.ReadFile(inputPath)
	if err != nil {
		fmt.Printf("error reading payload file: %v\n", err)
		os.Exit(1)
	}
	return func(i int, j int64) string {
		return string(content)
	}
}

type Worker struct {
	WorkerId             int
	BrokerUrl            string
	Username             string
	Password             string
	SkipTLSVerification  bool
	NumberOfMessages     int
	PayloadGenerator     PayloadGenerator
	Timestamp            int64
	Timeout              time.Duration
	Retained             bool
	PublisherQoS         byte
	SubscriberQoS        byte
	CA                   []byte
	Cert                 []byte
	Key                  []byte
	PauseBetweenMessages time.Duration
	DisableSub           bool
	Initialized          bool
	Queue                chan [2]string
	Subscriber           mqtt.Client
	Publisher            mqtt.Client
}

func setSkipTLS(o *mqtt.ClientOptions) {
	oldTLSCfg := o.TLSConfig
	if oldTLSCfg == nil {
		oldTLSCfg = &tls.Config{}
	}
	oldTLSCfg.InsecureSkipVerify = true
	o.SetTLSConfig(oldTLSCfg)
}

func NewTLSConfig(ca, certificate, privkey []byte) (*tls.Config, error) {
	// Import trusted certificates from CA
	certpool := x509.NewCertPool()
	ok := certpool.AppendCertsFromPEM(ca)

	if !ok {
		return nil, fmt.Errorf("CA is invalid")
	}

	// Import client certificate/key pair
	cert, err := tls.X509KeyPair(certificate, privkey)
	if err != nil {
		return nil, err
	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		// RootCAs = certs used to verify server cert.
		RootCAs: certpool,
		// ClientAuth = whether to request cert from server.
		// Since the server is set up for SSL, this happens
		// anyways.
		ClientAuth: tls.NoClientCert,
		// ClientCAs = certs used to validate client cert.
		ClientCAs: nil,
		// InsecureSkipVerify = verify that cert contents
		// match server. IP matches what is in cert etc.
		InsecureSkipVerify: false,
		// Certificates = list of certs client sends to server.
		Certificates: []tls.Certificate{cert},
	}, nil
}

func (w *Worker) Init(cid int, brokerUrl string, username string, password string,
	skipTLSVerification bool, num int, payloadGenerator PayloadGenerator, ts int64,
	actionTimeout time.Duration, retained bool, publisherQoS byte, subscriberQoS byte,
	ca []byte, cert []byte, key []byte, pauseBetweenMessages time.Duration, disableSub bool) error {

	verboseLogger.Printf("[%d] initializing\n", w.WorkerId)

	w.WorkerId = cid
	w.BrokerUrl = brokerUrl
	w.Username = username
	w.Password = password
	w.SkipTLSVerification = skipTLSVerification
	w.NumberOfMessages = num
	w.PayloadGenerator = payloadGenerator
	w.Timestamp = ts
	w.Timeout = actionTimeout
	w.Retained = retained
	w.PublisherQoS = publisherQoS
	w.SubscriberQoS = subscriberQoS
	w.CA = ca
	w.Cert = cert
	w.Key = key
	w.PauseBetweenMessages = pauseBetweenMessages
	w.DisableSub = disableSub

	w.Queue = make(chan [2]string)
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	clientId := randomSource.Int31()
	topicName := topicNameTemplate
	subscriberClientId := fmt.Sprintf(subscriberClientIdTemplate, hostname, w.WorkerId, clientId)
	publisherClientId := fmt.Sprintf(publisherClientIdTemplate, hostname, w.WorkerId, clientId)

	verboseLogger.Printf("[%d] topic=%s subscriberClientId=%s publisherClientId=%s\n", w.WorkerId, topicName, subscriberClientId, publisherClientId)

	publisherOptions := mqtt.NewClientOptions().SetClientID(publisherClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)
	subscriberOptions := mqtt.NewClientOptions().SetClientID(subscriberClientId).SetUsername(w.Username).SetPassword(w.Password).AddBroker(w.BrokerUrl)

	subscriberOptions.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		w.Queue <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	if len(w.CA) > 0 || len(w.Key) > 0 {
		tlsConfig, err := NewTLSConfig(w.CA, w.Cert, w.Key)
		if err != nil {
			panic(err)
		}
		subscriberOptions.SetTLSConfig(tlsConfig)
		publisherOptions.SetTLSConfig(tlsConfig)
	}

	if w.SkipTLSVerification {
		setSkipTLS(publisherOptions)
		setSkipTLS(subscriberOptions)
	}

	w.Subscriber = mqtt.NewClient(subscriberOptions)

	if !disableSub {
		verboseLogger.Printf("[%d] connecting subscriber\n", w.WorkerId)
		if token := w.Subscriber.Connect(); token.WaitTimeout(w.Timeout) && token.Error() != nil {
			resultChan <- Result{
				WorkerId:     w.WorkerId,
				Event:        ConnectFailedEvent,
				Error:        true,
				ErrorMessage: token.Error(),
			}
			return errors.New("fail to connect subscriber")
		}

		verboseLogger.Printf("[%d] subscribing to topic\n", w.WorkerId)
		if token := w.Subscriber.Subscribe(topicName, w.SubscriberQoS, nil); token.WaitTimeout(w.Timeout) && token.Error() != nil {
			resultChan <- Result{
				WorkerId:     w.WorkerId,
				Event:        SubscribeFailedEvent,
				Error:        true,
				ErrorMessage: token.Error(),
			}
			return errors.New("fail to subscribe to topic")
		}
	}

	w.Publisher = mqtt.NewClient(publisherOptions)
	verboseLogger.Printf("[%d] connecting publisher\n", w.WorkerId)
	if token := w.Publisher.Connect(); token.WaitTimeout(w.Timeout) && token.Error() != nil {
		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        ConnectFailedEvent,
			Error:        true,
			ErrorMessage: token.Error(),
		}
		return errors.New("fail to connect publisher")
	}

	w.Initialized = true

	return nil
}

func (w *Worker) Close() {
	if w.Publisher != nil && w.Publisher.IsConnected() {
		verboseLogger.Printf("[%d] disconnect\n", w.WorkerId)
		w.Publisher.Disconnect(5)
	}

	if w.Subscriber != nil && w.Subscriber.IsConnected() {
		verboseLogger.Printf("[%d] unsubscribe\n", w.WorkerId)
		topicName := topicNameTemplate
		if token := w.Subscriber.Unsubscribe(topicName); token.WaitTimeout(w.Timeout) && token.Error() != nil {
			fmt.Printf("failed to unsubscribe: %v\n", token.Error())
		}
		w.Subscriber.Disconnect(5)
	}
}

func (w *Worker) Run(cid int, brokerUrl string, username string, password string,
	skipTLSVerification bool, num int, payloadGenerator PayloadGenerator, ts int64,
	actionTimeout time.Duration, retained bool, publisherQoS byte, subscriberQoS byte,
	ca []byte, cert []byte, key []byte, pauseBetweenMessages time.Duration, disableSub bool, ctx context.Context) {

	fmt.Printf("%d worker started\n", cid)

	if !w.Initialized {
		err := w.Init(cid, brokerUrl, username, password,
			skipTLSVerification, num, payloadGenerator, ts,
			actionTimeout, retained, publisherQoS, subscriberQoS,
			ca, cert, key, pauseBetweenMessages, disableSub)
		if err != nil {
			fmt.Printf("[%d] failed to initialize: %s\n", cid, err)
			return
		}
	}

	topicName := topicNameTemplate
	verboseLogger.Printf("[%d] starting control loop %s\n", w.WorkerId, topicName)

	stopWorker := false
	receivedCount := 0
	publishedCount := 0

	t0 := time.Now()
	for i := 0; i < w.NumberOfMessages; i++ {
		text := w.PayloadGenerator(i, w.Timestamp)
		token := w.Publisher.Publish(topicName, w.PublisherQoS, w.Retained, text)
		publishedCount++
		token.WaitTimeout(w.Timeout)
		time.Sleep(w.PauseBetweenMessages)
	}
	publishTime := time.Since(t0)
	verboseLogger.Printf("[%d] all messages published\n", w.WorkerId)

	if w.DisableSub {
		resultChan <- Result{
			WorkerId:          w.WorkerId,
			Event:             CompletedEvent,
			PublishTime:       publishTime,
			ReceiveTime:       0,
			MessagesReceived:  receivedCount,
			MessagesPublished: publishedCount,
		}
		return
	}

	t0 = time.Now()
	for receivedCount < w.NumberOfMessages && !stopWorker {
		select {
		case <-w.Queue:
			receivedCount++

			verboseLogger.Printf("[%d] %d/%d received\n", w.WorkerId, receivedCount, w.NumberOfMessages)
			if receivedCount == w.NumberOfMessages {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             CompletedEvent,
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			} else {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             ProgressReportEvent,
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			}
		case <-ctx.Done():
			var event string
			var isError bool
			switch ctx.Err().(type) {
			case TimeoutError:
				verboseLogger.Printf("[%d] received abort signal due to test timeout", w.WorkerId)
				event = TimeoutExceededEvent
				isError = true
			default:
				verboseLogger.Printf("[%d] received abort signal", w.WorkerId)
				event = AbortedEvent
				isError = false
			}
			stopWorker = true
			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             event,
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             isError,
			}
		}
	}

	verboseLogger.Printf("[%d] worker finished\n", w.WorkerId)
}
