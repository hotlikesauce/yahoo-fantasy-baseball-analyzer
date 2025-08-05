FROM public.ecr.aws/lambda/python:3.9

# Copy function code and source
COPY lambda_handler.py ${LAMBDA_TASK_ROOT}
COPY src/ ${LAMBDA_TASK_ROOT}/src/

# Install dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set environment variables
ENV PYTHONPATH="${LAMBDA_TASK_ROOT}:${LAMBDA_TASK_ROOT}/src"

# Default handler
CMD [ "lambda_handler.lambda_handler_weekly" ]