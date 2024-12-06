import requests
from collections import Counter
from multiprocessing import Pool
import matplotlib.pyplot as plt


def fetch_text(url):
    """
    Fetches text content from the given URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching text from {url}: {e}")
        return ""


def map_words(chunk):
    """
    Maps words in a chunk of text to their frequencies.
    """
    words = chunk.lower().split()
    clean_words = [word.strip(".,!?\"':;()[]") for word in words if word.isalpha()]
    return Counter(clean_words)


def reduce_word_counts(counters):
    """
    Combines multiple Counter objects into a single Counter.
    """
    total_counter = Counter()
    for counter in counters:
        total_counter.update(counter)
    return total_counter


def split_text(text, num_chunks):
    """
    Splits the text into approximately equal-sized chunks.
    """
    words = text.split()
    chunk_size = len(words) // num_chunks
    return [" ".join(words[i * chunk_size:(i + 1) * chunk_size]) for i in range(num_chunks)]


def visualize_top_words(word_counts, top_n=10):
    """
    Visualizes the top N most frequent words in a bar chart.
    """
    top_words = word_counts.most_common(top_n)
    words, counts = zip(*top_words)

    plt.figure(figsize=(10, 6))
    plt.bar(words, counts, color="skyblue")
    plt.xlabel("Words")
    plt.ylabel("Frequency")
    plt.title(f"Top {top_n} Words by Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    print(f"Fetching text from {url}...")
    text = fetch_text(url)

    if not text:
        print("No text fetched. Exiting.")
        return

    print("Splitting text into chunks...")
    num_chunks = 8  # Number of chunks for parallel processing
    chunks = split_text(text, num_chunks)

    print("Processing text using MapReduce...")
    with Pool(processes=4) as pool:  # Use 4 parallel processes
        mapped_results = pool.map(map_words, chunks)

    print("Reducing results...")
    word_counts = reduce_word_counts(mapped_results)

    print("Visualizing top words...")
    visualize_top_words(word_counts, top_n=10)

if __name__ == "__main__":
    main()
