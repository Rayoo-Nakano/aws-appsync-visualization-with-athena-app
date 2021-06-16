/* Amplify Params - DO NOT EDIT
You can access the following resource attributes as environment variables from your Lambda function
var environment = process.env.ENV
var region = process.env.REGION
var apiAthenaGraphQlAPIGraphQLAPIIdOutput = process.env.API_APPSYNCATHENAVIZ_GRAPHQLAPIIDOUTPUT
var apiAthenaGraphQlAPIGraphQLAPIEndpointOutput = process.env.API_APPSYNCATHENAVIZ_GRAPHQLAPIENDPOINTOUTPUT

Amplify Params - DO NOT EDIT */

const AWS = require('aws-sdk')
const https = require('https')
const URL = require('url').URL
const mutation = require('./mutation').updateAthenaOperation

const apiRegion = process.env.REGION
const apiEndpoint = process.env.API_APPSYNCATHENAVIZ_GRAPHQLAPIENDPOINTOUTPUT
const endpoint = new URL(apiEndpoint).hostname.toString()

exports.handler = async (event, context) => {
  // Get the object from the event and show its content type
  const region = event.Records[0].awsRegion
  const bucket = event.Records[0].s3.bucket.name
  const key = event.Records[0].s3.object.key

  console.log('S3 Event -->')
  console.log(JSON.stringify({ region, bucket, key }, null, 2))

  const match = key.match(/([\w-]+)\.csv$/)
  if (!match) return
  const QueryExecutionId = match[1]

  const variables = {
    input: {
      id: QueryExecutionId,
      status: 'COMPLETED',
      file: {
        bucket,
        region,
        key
      }
    }
  }

  const req = new AWS.HttpRequest(apiEndpoint, apiRegion)
  req.method = 'POST'
  req.headers.host = endpoint
  req.headers['Content-Type'] = 'application/json'
  req.body = JSON.stringify({
    query: mutation,
    operationName: 'UpdateAthenaOperation',
    variables,
    authMode: 'AWS_IAM'
  })
  const signer = new AWS.Signers.V4(req, 'appsync', true)
  signer.addAuthorization(AWS.config.credentials, AWS.util.date.getDate())

  console.log('req -->', JSON.stringify(req, null, 2))

  const result = await new Promise((resolve, reject) => {
    const httpRequest = https.request({ ...req, host: endpoint }, result => {
      result.on('data', data => resolve(JSON.parse(data.toString())))
    })

    httpRequest.write(req.body)
    httpRequest.end()
  })
  console.log('data -->')
  console.log(JSON.stringify(result, null, 2))

  return
}
