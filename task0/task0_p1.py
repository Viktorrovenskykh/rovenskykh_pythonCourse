import numpy as np

def find_matching_rows(matrix):
    matching_rows = []
    for i in range(len(matrix)):
        if (matrix[i] == matrix[:, i]).all():
            matching_rows.append(i)
    return matching_rows

def sum_rows_with_negative(matrix):
    sum_rows = []
    for row in matrix:
        if np.any(row < 0):
            sum_rows.append(sum(row))
    return sum_rows



matrix = np.random.randint(-5, 10, size=(8, 8))
print("Матрица:")
print(matrix)

matching_rows = find_matching_rows(matrix)
print("\nСходящиеся строки и столбцы:")
for row in matching_rows:
    print(f"Row {row + 1} matches Column {row + 1}")

sum_negative_rows = sum_rows_with_negative(matrix)
if sum_negative_rows:
    print("\nСумма строк с отриц. элементами:")
    for i, row_sum in enumerate(sum_negative_rows):
        print(f"Рядок {i + 1}: {row_sum}")
else:
    print("\nНет рядков с отриц. элементами.")

