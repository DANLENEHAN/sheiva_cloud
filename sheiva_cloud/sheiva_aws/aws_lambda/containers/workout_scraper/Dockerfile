FROM public.ecr.aws/lambda/python:3.11

RUN yum install git -y

# Replace this id_rsa process at some point
COPY id_rsa /root/.ssh/id_rsa

RUN echo known_hosts > /root/.ssh/known_hosts
RUN chmod 600 /root/.ssh/id_rsa
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Add env vars
ENV MAIN_QUEUE="https://sqs.eu-west-1.amazonaws.com/381528172721/WorkoutLinkQueue"
ENV DEADLETTER_QUEUE_URL="https://sqs.eu-west-1.amazonaws.com/381528172721/WorkoutLinkQueueDeadLetter"
ENV BUCKET="sheiva-scraped-data"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.handler" ]
