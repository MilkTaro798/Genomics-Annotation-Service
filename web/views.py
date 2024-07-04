# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile

import urllib.parse
import os


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  if not bucket_name or not s3_key:
    abort(400, description="Missing required query parameters: 'bucket' or 'key'.")
  # Extract the job ID from the S3 key
  s3_key = urllib.parse.unquote_plus(s3_key)
  file_name = os.path.basename(s3_key)
  job_id = file_name.split('~')[0]
  folders = os.path.split(os.path.dirname(s3_key))  
  user_id = session['primary_identity']
  input_file_name = file_name.split('~')[1]
  user_email = session['email']
  # Persist job to database
  # Move your code here...
  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.WriteItem.html
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  submit_time = int(time.time())
  data = {
      "job_id": job_id,
      "user_id": user_id,
      "user_email": user_email,
      "input_file_name": input_file_name,
      "s3_inputs_bucket": bucket_name,
      "s3_key_input_file": s3_key,
      "submit_time": submit_time,
      "job_status": "PENDING"
  }
  table.put_item(Item=data)
  # Send message to request queue
  # Move your code here...
  # https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
  # https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
  sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  try:
      response = sns.publish(
          TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
          Message=json.dumps(data),
          MessageAttributes={
              'job_id': {'DataType': 'String', 'StringValue': job_id},
              'input_file_name': {'DataType': 'String', 'StringValue': input_file_name},
          }
      )
  except ClientError as e:
      app.logger.error(f"Failed to publish SNS message: {e}")
      return abort(500)

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session['primary_identity']
  # Get list of annotations to display
  # https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  response = table.query(
    IndexName='user_id_index',
    KeyConditionExpression=Key('user_id').eq(user_id)
  )
  annotations = response['Items']
  for annotation in annotations:
    submit_time = annotation['submit_time']
    formatted_submit_time = datetime.fromtimestamp(submit_time).strftime('%Y-%m-%d %H:%M')
    annotation['submit_time'] = formatted_submit_time
  # Sort the annotations based on 'submit_time' in descending order
  sorted_annotations = sorted(annotations, key=lambda x: x['submit_time'], reverse=True)
  return render_template('annotations.html', annotations=sorted_annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  # Fetch the annotation job details from the DynamoDB table
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
  response = table.get_item(Key={'job_id': id})
  annotation = response.get('Item')
  if not annotation:
    return render_template('error.html', title='Not found', alert_level='warning', message="The requested annotation job does not exist."), 404
  # Check if the job belongs to the authenticated user
  if annotation['user_id'] != user_id:
    return render_template('error.html', title='Not authorized', alert_level='danger', message="You are not authorized to view this annotation job."), 403
  # Convert epoch times to human-readable format
  submit_time = annotation['submit_time']
  annotation['submit_time'] = datetime.fromtimestamp(submit_time).strftime('%Y-%m-%d %H:%M')
  free_access_expired = False
  if 'complete_time' in annotation:
    complete_time = annotation['complete_time']
    annotation['complete_time'] = datetime.fromtimestamp(complete_time).strftime('%Y-%m-%d %H:%M')
    
  # Generate presigned URLs for input, result, and log files
  # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
  s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version='s3v4'))
  input_file_key = annotation['s3_key_input_file']
  annotation['input_file_url'] = s3.generate_presigned_url('get_object', Params={'Bucket': app.config['AWS_S3_INPUTS_BUCKET'], 'Key': input_file_key}, ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  if annotation['job_status'] == 'COMPLETED':
    if ('results_file_archive_id' in annotation) and (session.get('role') == "free_user") :
      free_access_expired = True
    elif 'restore_job_id' in annotation:
      annotation['restore_message'] = 'Your file is being processed. This may take 4-5 hours. Please be patient!'
    else: 
      result_file_key = annotation['s3_key_result_file']
      annotation['result_file_url'] = s3.generate_presigned_url('get_object', Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'], 'Key': result_file_key}, ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    log_file_key = annotation['s3_key_log_file']
    annotation['log_file_url'] = s3.generate_presigned_url('get_object', Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'], 'Key': log_file_key}, ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  return render_template('annotation_details.html', annotation=annotation, free_access_expired = free_access_expired)
                           
"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  user_id = session['primary_identity']
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  # Fetch the annotation job details from the DynamoDB table
  # https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
  response = table.get_item(Key={'job_id': id})
  annotation = response.get('Item')
  if not annotation:
    return render_template('error.html', title='Not found', alert_level='warning', message="The requested annotation job does not exist."), 404
  # Check if the job belongs to the authenticated user
  if annotation['user_id'] != user_id:
    return render_template('error.html', title='Not authorized', alert_level='danger', message="You are not authorized to view this annotation job."), 403
  # Check if the job is completed
  if annotation['job_status'] != 'COMPLETED':
    return render_template('error.html', title='Job not completed', alert_level='warning', message="The annotation job has not been completed yet."), 400
  # Generate a presigned URL for the log file
  # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
  s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'], config=Config(signature_version='s3v4'))
  log_file_key = annotation['s3_key_log_file']
  log_file_url = s3.generate_presigned_url('get_object', Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'], 'Key': log_file_key}, ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  # Fetch the contents of the log file from S3
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
  # https://medium.com/@rohitshrivastava87/2-how-to-read-data-from-s3-bucket-using-python-945324d73d61
  log_file_response = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=log_file_key)
  log_file_contents = log_file_response['Body'].read().decode('utf-8')
    
  return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)

"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    # https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html
    dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    try:
      response = table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(session['primary_identity'])
      )
      items = response['Items']
      # Initialize SNS client
      sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
      # https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
      # Publish messages for each job that has results_file_archive_id
      for item in items:
        if 'results_file_archive_id' in item:
          item['submit_time'] = int(item['submit_time'])
          item['complete_time'] = int(item['complete_time'])
          message = json.dumps(item)

          try:
            sns.publish(
              TopicArn=app.config['AWS_SNS_JOB_RESTORE_TOPIC'],
              Message=message,
              MessageAttributes={
                'job_id': {'DataType': 'String', 'StringValue': item['job_id']}
              }
            )
            app.logger.info(f"Published restore message for job {item['job_id']}")
          except ClientError as e:
            app.logger.error(f"Failed to publish SNS restore message for job {item['job_id']}: {e}")
          
    except ClientError as e:
        app.logger.error(f"Unable to query data: {e}")
        return abort(500)

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
