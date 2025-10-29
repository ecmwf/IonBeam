import uvicorn

from . import create_legacy_application


def main() -> None:
    app = create_legacy_application()
    
    # Run the server with sensible defaults
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
    )


if __name__ == "__main__":
    main()