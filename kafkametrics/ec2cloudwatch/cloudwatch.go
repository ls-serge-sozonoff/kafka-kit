package ec2cloudwatch

import (
	"fmt"
	"github.com/DataDog/kafka-kit/v3/kafkametrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/davecgh/go-spew/spew"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds Handler configuration parameters.
type Config struct {
	// AWS Region
	AWSRegion string

	// BrokerIDTag is the host tag name for Kafka broker IDs.
	// broker-id
	BrokerIDTag string
}

type awsHandler struct {
	CloudwatchService *cloudwatch.CloudWatch
	EC2Service        *ec2.EC2
	BrokerIDTag       string
}

type Brokerinfo struct {
	broker    kafkametrics.Broker
	InstaceId string
}

func NewHandler(c *Config) (kafkametrics.Handler, error) {
	AWSsession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	h := &awsHandler{

		CloudwatchService: cloudwatch.New(AWSsession, aws.NewConfig().WithRegion(c.AWSRegion)),
		EC2Service:        ec2.New(AWSsession, aws.NewConfig().WithRegion(c.AWSRegion)),
	}
	h.BrokerIDTag = c.BrokerIDTag
	return h, nil
}

func (h *awsHandler) PostEvent(e *kafkametrics.Event) error {
	// noop
	return nil
}

func (h *awsHandler) GetMetrics() (kafkametrics.BrokerMetrics, []error) {

	var brokerMap = make(map[int]*(Brokerinfo))

	/*
		flag.StringVar(&bucket, "b", "", "Bucket name.")
		flag.StringVar(&key, "k", "", "Object key name.")
		flag.DurationVar(&timeout, "d", 0, "Upload timeout.")
		flag.Parse() */

	resp, err := h.EC2Service.DescribeInstances(&ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("tag:" + h.BrokerIDTag),
				Values: []*string{aws.String("*")},
			},
			{
				Name:   aws.String("instance-state-name"),
				Values: []*string{aws.String("running")},
			},
		},
	})
	if err != nil {
		fmt.Println("there was an error listing instances in", err.Error())
		log.Fatal(err.Error())
	}

	for idx, _ := range resp.Reservations {
		for _, inst := range resp.Reservations[idx].Instances {
			broker := Brokerinfo{}
			broker.InstaceId = *inst.InstanceId
			broker.broker.Host = *inst.PrivateDnsName
			broker.broker.InstanceType = *inst.InstanceType

			for _, tag := range inst.Tags {
				if *tag.Key == h.BrokerIDTag {
					broker.broker.ID, _ = strconv.Atoi(*tag.Value)
				}
			}

			brokerMap[broker.broker.ID] = &broker
		}
	}

	n := time.Now()
	d, _ := time.ParseDuration("-5m")

	queries := make([]*cloudwatch.MetricDataQuery, len(brokerMap)*2)

	idx := 0
	for key, instance := range brokerMap {
		queries[idx] = &cloudwatch.MetricDataQuery{
			Id:    aws.String("out_" + strconv.Itoa(key)),
			Label: aws.String("kafkaNetOut_" + instance.InstaceId),
			MetricStat: &cloudwatch.MetricStat{
				Period: aws.Int64(1),
				Stat:   aws.String("Average"),

				Metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/EC2"),
					MetricName: aws.String("NetworkOut"),

					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("InstanceId"),
							Value: aws.String(instance.InstaceId),
						},
					},
				},
			},
		}

		queries[idx+len(brokerMap)] = &cloudwatch.MetricDataQuery{
			Id:    aws.String("in_" + strconv.Itoa(key)),
			Label: aws.String("kafkaNetIn_" + instance.InstaceId),
			MetricStat: &cloudwatch.MetricStat{
				Period: aws.Int64(1),
				Stat:   aws.String("Average"),

				Metric: &cloudwatch.Metric{
					Namespace:  aws.String("AWS/EC2"),
					MetricName: aws.String("NetworkIn"),

					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  aws.String("InstanceId"),
							Value: aws.String(instance.InstaceId),
						},
					},
				},
			},
		}
		idx++
	}

	metrics, err := h.CloudwatchService.GetMetricData(&cloudwatch.GetMetricDataInput{
		EndTime:           aws.Time(n),
		StartTime:         aws.Time(n.Add(d)),
		MetricDataQueries: queries,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error - ", err)
		os.Exit(1)
	}

	//fmt.Println("Metric Result", metrics.MetricDataResults)
	for _, s := range metrics.MetricDataResults {
		info := strings.Split(*s.Id, "_")
		id, _ := strconv.Atoi(info[1])
		if info[0] == "in" {
			brokerMap[id].broker.NetRX = (*s.Values[0] / 60)
		} else {
			brokerMap[id].broker.NetTX = (*s.Values[0] / 60)
		}

		//fmt.Println(*s.Label + " -> ", (*s.Values[0]/60), "MB/s")
	}

	m := map[int]*kafkametrics.Broker{}
	for k, v := range brokerMap {
		m[k] = &v.broker
	}

	spew.Dump(brokerMap)
	spew.Dump(m)

	return m, nil
}
