import numpy as np

def count_zeros(matrix):
    count = 0
    for row in matrix:
        if 0 in row:
            count += 1
    return count

def longest_series_column(matrix):
    longest_series_col = 0
    max_series_length = 0
    for j in range(matrix.shape[1]):
        current_length = 1
        for i in range(1, matrix.shape[0]):
            if matrix[i, j] == matrix[i-1, j]:
                current_length += 1
            else:
                if current_length > max_series_length:
                    max_series_length = current_length
                    longest_series_col = j
                current_length = 1
        if current_length > max_series_length:
            max_series_length = current_length
            longest_series_col = j
    return longest_series_col


matrix = np.random.randint(-2, 2, size=(10, 10))
print("Матрица:")
print(matrix)

zero_rows_count = count_zeros(matrix)
print("\nКоличество строк с нулевыми элементами:", zero_rows_count)

longest_series_col = longest_series_column(matrix)
print("\nНомер столбца с самой длинной серией одинаковых элементов:", longest_series_col)


