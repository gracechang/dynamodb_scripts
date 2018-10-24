package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type Row struct {
	// caps becuase of: https://stackoverflow.com/questions/49036888/create-item-in-dynamodb-using-go
	COLUMN_NAME_A string `json:"column_name_a"`
	COLUMN_NAME_B int64  `json:"column_name_b"`
	COLUMN_NAME_C string `json:"column_name_c"`
}

func main() {
	counter := 0
	filename := "/tmp/csv_file.csv"

	// Open CSV file
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll() // if the file is too big then should just use Read()
	if err != nil {
		panic(err)
	}

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	// Loop through lines & turn into object
	for _, line := range lines {
		data := Row{
			COLUMN_NAME_A: line[0],
			COLUMN_NAME_B: line[1],
			COLUMN_NAME_C: line[3],
		}
		av, err := dynamodbattribute.MarshalMap(data)

		if err != nil {
			fmt.Println("Got error creating attributes.")
			fmt.Println(err.Error())
			os.Exit(1)
		}
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String("dynamo_table_name"),
		}

		_, err = svc.PutItem(input)

		if err != nil {
			fmt.Println("Got error calling sending the item to Dyanamo")
			fmt.Println(err.Error())
			os.Exit(1)
		}
		if counter%1000 == 0 {
			fmt.Println("Sent ", counter, " at ", makeTimestamp())
		}
	}

}
func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
