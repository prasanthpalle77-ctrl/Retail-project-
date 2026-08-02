.PHONY: install validate bundle-validate bundle-deploy

install:
	python -m pip install -r requirements.txt

validate:
	python scripts/validate_environment.py

bundle-validate:
	databricks bundle validate --target prod

bundle-deploy:
	databricks bundle deploy --target prod
