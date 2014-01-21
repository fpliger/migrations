from migrations import backends

def test_backends():
	BACKENDS = ["csv", "json", "excel", "dbms"]
	METHODS = ["dump"]

	for backend in BACKENDS:
		backend = getattr(backends, backend, None)

		assert backend is not None

		for method_name in METHODS:
			method = getattr(backend, method_name, None)

			assert callable(method)