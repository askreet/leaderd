package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"os"
	"strconv"
	"time"
)

var table string
var region string
var name string

var interval int
var timeout int

var dynamo *dynamodb.DynamoDB

var leader = "unknown-leader"

func main() {
	if err := parseArguments(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	dynamo = dynamodb.New(session.New())

	var currentLeader *CurrentLeader
	var err error

	for {
		if leader == name {
			updateLastUpdate()
		} else {
			currentLeader, err = getCurrentLeader()

			if err != nil {
				log.Printf("Failed to query current leader: %s.", err.Error())

				time.Sleep(time.Duration(interval) * time.Second)
				continue
			} else {
				if currentLeader.Name != leader {
					log.Printf("Leader has changed from %s to %s.", leader, currentLeader.Name)
				}

				leader = currentLeader.Name
			}

			// If the current leader has expired, try to steal leader.
			if currentLeader.Name != name && currentLeader.LastUpdate <= time.Now().Unix()-int64(timeout) {
				log.Printf("Attempting to steal leader from expired leader %s.", currentLeader.Name)
				err = attemptToStealLeader()
				if err != nil {
					log.Print("Success! This node is now the leader.")
					leader = name
				} else {
					log.Printf("Error while stealing leadership role: %s", err)
				}
			}

			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
}

func parseArguments() error {
	flag.StringVar(&table, "table", "", "dynamodb table to use")
	flag.StringVar(&name, "name", "", "name for this node")
	flag.IntVar(&interval, "interval", 10, "how often (seconds) to check if leader can be replaced, or to update leader timestamp if we are leader")
	flag.IntVar(&timeout, "timeout", 60, "number of seconds before attempting to steal leader")

	flag.Parse()

	if table == "" {
		return errors.New("required argument table not provided")
	}

	if name == "" {
		return errors.New("required argument name not provided")
	}

	return nil
}

type CurrentLeader struct {
	Set        bool
	Name       string
	LastUpdate int64
}

func getCurrentLeader() (*CurrentLeader, error) {
	result, err := dynamo.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			"LockName": &dynamodb.AttributeValue{S: aws.String("Leader")},
		},
	})

	var lastUpdate int64
	if val, ok := result.Item["LastUpdate"]; ok {
		lastUpdate, err = strconv.ParseInt(*val.N, 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		// Leader has not been properly set.
		return &CurrentLeader{Set: false}, nil
	}

	var leaderName string
	if val, ok := result.Item["LeaderName"]; ok {
		leaderName = *val.S
	} else {
		return &CurrentLeader{Set: false}, nil
	}

	currentLeader := &CurrentLeader{
		Set:        true,
		Name:       leaderName,
		LastUpdate: lastUpdate,
	}

	return currentLeader, nil
}

func attemptToStealLeader() error {
	expiry := strconv.FormatInt(time.Now().Unix()-int64(timeout), 10)
	now := strconv.FormatInt(time.Now().Unix(), 10)

	_, err := dynamo.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]*dynamodb.AttributeValue{
			"LockName":   &dynamodb.AttributeValue{S: aws.String("Leader")},
			"LeaderName": &dynamodb.AttributeValue{S: aws.String(name)},
			"LastUpdate": &dynamodb.AttributeValue{N: aws.String(now)},
		},
		// Only take leadership if no leader is assigned, or if the current leader
		// hasn't checked in in the last +timeout+ seconds.
		ConditionExpression: aws.String("attribute_not_exists(LeaderName) OR LastUpdate <= :expiry"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":expiry": &dynamodb.AttributeValue{N: aws.String(expiry)},
		},
		ReturnValues: aws.String("ALL_OLD"),
	})

	return err
}

// If we are the current leader, keep LastUpdate up-to-date,
// so that no one steals our title.
func updateLastUpdate() error {
	now := strconv.FormatInt(time.Now().Unix(), 10)

	_, err := dynamo.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]*dynamodb.AttributeValue{
			"LockName":   &dynamodb.AttributeValue{S: aws.String("Leader")},
			"LeaderName": &dynamodb.AttributeValue{S: aws.String(name)},
			"LastUpdate": &dynamodb.AttributeValue{N: aws.String(now)},
		},
		ConditionExpression: aws.String("LeaderName = :name"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name": &dynamodb.AttributeValue{S: aws.String(name)},
		},
	})
	if err != nil {
		// TODO: If the condition expression fails, we've lost our leadership.
		// We'll have to convert this error and test for that failure.
		log.Printf("updateLastUpdate(): %#v", err)
		log.Print(err.Error())
		return err
	}

	return nil
}
