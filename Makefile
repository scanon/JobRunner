.PHONY: test

docker:
	docker build -t kbase/indexrunner .

runtester: test/runtester.ready

test/runtester.ready:
	(cd test && git clone https://github.com/kbaseapps/RunTester && cd RunTester && docker build -t test/runtester .)
	touch runtester.ready

mock:
	docker build -t mock_app ./test/mock_app

test:
	nosetests -A "not online" -s -x -v --with-coverage --cover-package=JobRunner --cover-erase --cover-html --cover-html-dir=./test_coverage --nocapture  --nologcapture .


clean:
	rm -rfv $(LBIN_DIR)

