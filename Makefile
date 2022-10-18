.DEFAULT_GOAL := all
isort = isort hsmodels tests
black = black -S -l 120 --target-version py38 hsextract tests

.PHONY: format
format:
	$(isort)
	$(black)
