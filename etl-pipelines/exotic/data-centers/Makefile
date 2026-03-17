.PHONY: run clean

run:
	python src/data_center_etl.py \
		--input sample_data/raw_facilities.csv \
		--output output

clean:
	rm -rf output/bronze output/silver output/gold
