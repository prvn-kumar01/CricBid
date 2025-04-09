from pymongo import MongoClient

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["cricket_bid"]
comments_collection = db["comments"]

def add_comment():
    player_id = input("Enter Player ID: ")
    user = input("Enter Your Username: ")
    comment_text = input("Enter Your Comment: ")

    comment = {
        "player_id": player_id,
        "user": user,
        "comment_text": comment_text,
        "likes": 0,
        "replies": []
    }

    comments_collection.insert_one(comment)
    print("✅ Comment added successfully!")

def get_comments():
    player_id = input("Enter Player ID to view comments: ")
    comments = comments_collection.find({"player_id": player_id})

    found = False
    for comment in comments:
        print(comment)
        found = True

    if not found:
        print("⚠️ No comments found for this Player ID.")

def main():
    while True:
        print("\nChoose an option:")
        print("1. Add a Comment")
        print("2. View Comments")
        print("3. Exit")

        choice = input("Enter your choice (1/2/3): ")

        if choice == '1':
            add_comment()
        elif choice == '2':
            get_comments()
        elif choice == '3':
            print("Exiting the application. Goodbye!")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    main()