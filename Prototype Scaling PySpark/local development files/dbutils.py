class MockDbUtils:
    class Fs:
        @staticmethod
        def ls(path):
            # Return a mock list of files for local testing
            return []

        @staticmethod
        def mkdirs(path):
            # Simulate creating directories
            print(f"Mock mkdirs called for path: {path}")

        @staticmethod
        def rm(path, recurse=False):
            # Simulate removing files
            print(f"Mock rm called for path: {path}, recurse: {recurse}")

    fs = Fs()