//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <iostream>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	std::cout << "encoding="<< encoding << std::endl;
    if(1) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		this->dates = nullptr;
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
    std::cout << "Set called: elementNum = " << elementNum << ", days = " << days << std::endl;

    // 输出当前的 writeIndex 和 dates 数组的长度
    std::cout << "Current writeIndex = " << writeIndex << ", Length of dates array = " << length << std::endl;

    // 如果 elementNum 超出了当前写入索引，需要更新 writeIndex
    if (elementNum >= writeIndex) {
        std::cout << "Updating writeIndex from " << writeIndex << " to " << elementNum + 1 << std::endl;
        writeIndex = elementNum + 1;
    }

    // 检查 dates 是否为空指针
    if (dates == nullptr) {
        std::cout << "Error: dates array is nullptr!" << std::endl;
    } else {
        std::cout << "Setting value in dates[" << elementNum << "] = " << days << std::endl;
        dates[elementNum] = days;
    }

    // 检查 isNull 数组是否为空
    if (isNull == nullptr) {
        std::cout << "Error: isNull array is nullptr!" << std::endl;
    } else {
        std::cout << "Setting isNull[" << elementNum << "] = false" << std::endl;
        isNull[elementNum] = false;
    }

    std::cout << "After setting, dates[" << elementNum << "] = " << dates[elementNum] << ", isNull[" << elementNum << "] = " << isNull[elementNum] << std::endl;
}




void DateColumnVector::add(std::string &value) {
    std::cout << "Entering add function" << std::endl;
    std::cout << "Value address: " << &value << ", size: " << value.size() << std::endl;
    std::cout<<"value="<<value<<std::endl;
	std::cout << "Step 1: Check format and length" << std::endl;
    if (value.size() != 10 || value[4] != '-' || value[7] != '-') {
        throw std::invalid_argument("Invalid date format, expected YYYY-MM-DD");
    }

    std::cout << "Step 2: Extract year, month, day" << std::endl;
    int year = (value[0] - '0') * 1000 + (value[1] - '0') * 100 +
               (value[2] - '0') * 10 + (value[3] - '0');
    int month = (value[5] - '0') * 10 + (value[6] - '0');
    int day = (value[8] - '0') * 10 + (value[9] - '0');

    std::cout << "Step 3: Check month and day validity" << std::endl;
    if (month < 1 || month > 12 || day < 1 || day > 31) {
        throw std::invalid_argument("Invalid date value");
    }

    auto isLeapYear = [](int y) {
        return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    };

    std::cout << "Step 4: Check specific day validity" << std::endl;
    int daysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month == 2 && isLeapYear(year)) {
        if (day > 29) {
            throw std::invalid_argument("Invalid date value for leap year");
        }
    } else if (day > daysInMonth[month - 1]) {
        throw std::invalid_argument("Invalid date value");
    }

    std::cout << "Step 5: Calculate total days since 1970" << std::endl;
    int totalDays = 0;

    for (int y = 1970; y < year; ++y) {
        totalDays += isLeapYear(y) ? 366 : 365;
    }

    for (int m = 1; m < month; ++m) {
        totalDays += daysInMonth[m - 1];
        if (m == 2 && isLeapYear(year)) {
            totalDays += 1;
        }
    }

    totalDays += day - 1;

    std::cout << "Step 6: Final totalDays: " << totalDays << std::endl;

    add(totalDays);
}

void DateColumnVector::add(bool value)
{
	add(value ? 1 : 0);
}
void DateColumnVector::add(int64_t value)
{
	add(static_cast<int>(value));  
}
void DateColumnVector::add(int value)
{
	if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    set(index, value);
}
void DateColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    std::cout << "ensureSize called: Requested size = " << size << ", Current length = " << length << std::endl;

    // 调用基类的 ensureSize
    ColumnVector::ensureSize(size, preserveData);

    // 判断当前长度是否小于请求的大小
    if (length < size) {
        std::cout << "Current length is smaller than requested size. Expanding size." << std::endl;

        int *oldVector = dates;
        std::cout << "Allocating new memory for dates array with size: " << size * sizeof(int32_t) << " bytes" << std::endl;

        // 分配新的内存块
        posix_memalign(reinterpret_cast<void **>(&dates), 32, size * sizeof(int32_t));
        if (dates == nullptr) {
            std::cout << "Error: Memory allocation failed for dates array!" << std::endl;
            return;
        }

        // 如果需要保留数据，将旧数据拷贝到新内存中
        if (preserveData) {
            std::cout << "Preserving existing data..." << std::endl;
            std::copy(oldVector, oldVector + length, dates);
        }

        // 释放旧的内存
        std::cout << "Freeing old memory" << std::endl;
        delete[] oldVector;

        // 更新内存使用量
        memoryUsage += static_cast<long>(sizeof(int) * (size - length));
        std::cout << "Updated memory usage: " << memoryUsage << " bytes" << std::endl;

        // 调整数组的大小
        resize(size);
        std::cout << "Resized dates array to new size: " << size << std::endl;
    } else {
        std::cout << "No resizing needed. Current length is sufficient." << std::endl;
    }
}

void *DateColumnVector::current()
{
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}
