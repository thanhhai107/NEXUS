import argparse
import requests


def fetch_url(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SimpleFetcher/1.0)"
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=20
    )

    print("URL:", response.url)
    print("Status code:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))

    response.raise_for_status()

    return response.text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="URL cần GET")
    parser.add_argument("--output", default="output.html", help="File lưu nội dung")

    args = parser.parse_args()

    content = fetch_url(args.url)

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Đã lưu nội dung vào: {args.output}")


if __name__ == "__main__":
    main()