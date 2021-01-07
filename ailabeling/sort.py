

def sort(list):
    list.sort(key=lambda x: (int(x.split('_')[0]), int(x.split('_')[2])))

    print(list)


if __name__ == "__main__":
    list = ["1_front_1_038-23011016201202092437539.jpg", "1_front_4_038-23011016201202092443539.jpg",
            "1_front_3_038-23011016201202092441539.jpg","1_front_30_038-23011016201202092441539.jpg",
            "2_front_4_038-23011016201202092520565.jpg", "2_front_1_038-23011016201202092452440.jpg", ]

    sort(list)