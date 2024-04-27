import numpy as np

def minimal(matrix):
    local_min_count = 0
    for i in range(1, matrix.shape[0] - 1):
        for j in range(1, matrix.shape[1] - 1):
            if matrix[i, j] < matrix[i-1:i+2, j-1:j+2].min():
                local_min_count += 1
    return local_min_count

def sum(matrix):
    sum_absolute_values = np.sum(np.abs(matrix[np.triu_indices(matrix.shape[0], k=1)]))
    return sum_absolute_values



matrix = np.random.randint(-10, 10, size=(10, 10))
print("Матрица:")
print(matrix)

local_min_count = minimal(matrix)
print("\nКолл-во локальных минимумов", local_min_count)

sum_abs_above_diagonal = sum(matrix)
print("\nСумма абсолютных значений выше главной диагонали:", sum_abs_above_diagonal)


