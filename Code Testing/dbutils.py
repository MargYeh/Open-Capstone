class MockDbUtils:
    class fs:
        @staticmethod
        def cp(src, dest):
            print(f"Mock copy from {src} to {dest}")