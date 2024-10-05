FEATURES	=

USE_BITMAP_U64	?= 0
ifeq (${USE_BITMAP_U64}, 1)
	FEATURES	+= --features=bitmap_u64
endif

bin:
	cargo build --release ${FEATURES}
clean:
	cargo clean --release -p libexfat
clean_all:
	cargo clean
fmt:
	cargo fmt
	git status
lint:
	cargo clippy --release --fix --all ${FEATURES}
	git status
plint:
	cargo clippy --release --fix --all ${FEATURES} -- -W clippy::pedantic
	git status
test:
	cargo test --release ${FEATURES}
test_debug:
	cargo test --release ${FEATURES} -- --nocapture

xxx:	fmt lint test
