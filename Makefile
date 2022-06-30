SRC_PATH=./src
SAMPLE_URI=https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json
DEV_FOLDER=/tmp/run/input
s3_DEST_PATH=

prepare:
	mkdir -p ${DEV_FOLDER}
	wget ${SAMPLE_URI} -O ${DEV_FOLDER}/recipes.json
	python3 -m pip install --user pypandoc
	python3 -m pip install --user -r requirements.txt
	cd ${SRC_PATH} && python3 -m pip install --user -e .

prepare-tests:
	mkdir -p ${DEV_FOLDER}/
	wget ${SAMPLE_URI} -O ${DEV_FOLDER}/recipes.json
	python3 -m pip install --user pypandoc
	python3 -m pip install --user -r requirements.tests.txt
	cd ${SRC_PATH} && python3 -m pip install --user -e .

task1:
	cd ${SRC_PATH} && PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 spark-submit spark_jobs/recipe_history_ingestion.py -C config/task_1.ini

task2:
	cd ${SRC_PATH} && PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 spark-submit spark_jobs/average_difficulty_by_ingredients_report.py -C config/task_2.ini

tests:
	cd ${SRC_PATH}/tests && PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 python3 -m pytest -W ignore::DeprecationWarning

deploy:
	aws s3 sync ./src/spark_jobs ${s3_DEST_PATH}/spark_jobs/
	aws s3 sync ./src/great_expectations ${s3_DEST_PATH}/great_expectations/
	aws s3 sync ./src/setup.py ${s3_DEST_PATH}/setup.py