lint:
	ruff check .

lint-fix:
	ruff check . --fix

test:
	pytest tests/ -v

smoke:
	python -c "import telegram_meeting_bot"

docker-build:
	docker build -t slonyara-bot:ci .
