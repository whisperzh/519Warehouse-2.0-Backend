FROM public.ecr.aws/lambda/python:3.9

COPY Tasks/requirements.txt ./
RUN python3.9 -m pip install -r requirements.txt -t .

COPY Tasks/app.py ./

# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]
