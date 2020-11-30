import database
import calculate
import visualizations


def main():
    while True:
        print("Hello, world!")
        print()

        choice = int()
        print("1. Create databases")
        print("2. Update tables")
        print("0. To quit")

        try:
            choice = int(input())
        except ValueError as e:
            print(f"Error: {e}... please enter a number!")
            print()
            continue

        if choice == 1:
            database.create_databases()
            print()
            continue
        elif choice == 2:
            inner_choice = int()
            tracker = 0
            print("Which table to update?")
            print("1. Global table")
            print("2. Country table")
            print("3. USA table")
            print("4. USA state table")
            print("5. Delete rows")
            print("6. Start over")
            print("7. Start ALL over")
            print("0. To quit")
            try:
                inner_choice = int(input())
            except ValueError as e:
                print(f"Error {e}... please enter a number!")
                print()
                continue
            if inner_choice == 1:
                if tracker >= 1:
                    print(
                        "You've updated this table too many times today. Please update tomorrow.")
                    break
                else:
                    database.save_global_data()
                    tracker += 1
            elif inner_choice == 2:
                if tracker >= 3:
                    print(
                        "You've updated this table too many times today. Please update tomorrow.")
                    break
                else:
                    database.save_country_data()
                    tracker += 1
                    count = 3 - tracker
                    print(f"Done! You can do this {count} more times")
            elif inner_choice == 3:
                if tracker >= 1:
                    print(
                        "You've updated this table too many times today. Please update tomorrow")
                    break
                else:
                    database.save_usa_data()
                    tracker += 1
            elif inner_choice == 4:
                if tracker >= 3:
                    print(
                        "You've updated this table too many times today. Please update tomorrow")
                    break
                else:
                    database.save_states_data()
                    count = 3 - tracker
                    print(f"Done! You can do this {count} more times")
            elif inner_choice == 5:
                print("Which table would you like to delete rows from?")
                table = input()
                print("Up to which row would you like to keep?")
                rows = int(input())
                database.delete_rows(table, rows)
            elif inner_choice == 6:
                print("Which table would you like to refresh?")
                print("Countries? Global? States? USA?")
                table = input()
                database.start_over(table)
            elif inner_choice == 7:
                print("WARNING. This will delete ALL TABLES! ARE YOU SURE? Y/N")
                sure = input()
                if sure == 'Y' or sure == 'y':
                    database.start_all_over()
                else:
                    continue
            elif inner_choice == 0:
                print("Goodbye!")
                print()
                break
        elif choice == 0:
            print("Goodbye!")
            print()
            break


if __name__ == '__main__':
    main()
