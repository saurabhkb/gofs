alice:
	go run main.go -debug -name alice

bob:
	go run main.go -debug -name bob

charlie:
	go run main.go -debug -name charlie

clean:
	rm -rf /tmp/db
