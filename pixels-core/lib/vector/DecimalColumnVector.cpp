//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"
#include <complex>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

void DecimalColumnVector::add(std::string &value) {
    std::cout << "[DEBUG] Entered add(std::string &value), input: " << value << std::endl;

    int pos = value.find('.');  // 找到小数点位置
    try {
        // 如果没有小数点，则认为是整数输入
        if (pos == std::string::npos) {
            int64_t lv = std::stoll(value);  // 使用 stoll 转换为整数
            int64_t scaled_value = lv * std::pow(10, scale);
            
            std::cout << "[DEBUG] Integer input: lv = " << lv << ", scale = " << scale 
                      << ", scaled_value = " << scaled_value << std::endl;

            if (scaled_value / std::pow(10, scale) != lv) {  // 检测溢出
                std::cerr << "[DEBUG] Detected overflow. lv: " << lv 
                          << ", scaled_value: " << scaled_value 
                          << ", scale: " << scale 
                          << ", max allowed value for int64_t: " << INT64_MAX << std::endl;
                throw std::overflow_error("Overflow detected during scaling");
            }

            std::cout << "[DEBUG] Detected integer input. lv: " << lv 
                      << ", scaled value: " << scaled_value << std::endl;

            // 根据 precision 选择存储类型
            using duckdb::Decimal;
            if (precision <= Decimal::MAX_WIDTH_INT16) {
                add(static_cast<int16_t>(scaled_value));
            } else if (precision <= Decimal::MAX_WIDTH_INT32) {
                add(static_cast<int32_t>(scaled_value));
            } else if (precision <= Decimal::MAX_WIDTH_INT64) {
                add(scaled_value);
            } else {
                // 如果 precision > INT64
                add(static_cast<int64_t>(scaled_value));  // 使用 INT64 类型作为默认
            }

        } else {
            // 否则认为是小数输入
            std::string integer_part = value.substr(0, pos);  // 小数点前的整数部分
            std::string fractional_part = value.substr(pos + 1);  // 小数点后的部分
            
            int64_t lv = std::stoll(integer_part + fractional_part);  // 连接整数部分和小数部分
            int64_t scaled_value = lv * std::pow(10, scale - fractional_part.length());  // 缩放
            
            std::cout << "[DEBUG] Decimal input: integer_part = " << integer_part 
                      << ", fractional_part = " << fractional_part 
                      << ", lv = " << lv 
                      << ", scale = " << scale 
                      << ", scaled_value = " << scaled_value << std::endl;

            if (scale - fractional_part.length() < 0 || scaled_value / std::pow(10, scale - fractional_part.length()) != lv) {
                std::cerr << "[DEBUG] Detected overflow. lv: " << lv 
                          << ", scaled_value: " << scaled_value 
                          << ", scale: " << scale 
                          << ", fractional_part.length(): " << fractional_part.length() 
                          << ", max allowed value for int64_t: " << INT64_MAX << std::endl;
                throw std::overflow_error("Overflow detected during scaling");
            }

            std::cout << "[DEBUG] Detected decimal input. lv: " << lv 
                      << ", scaled value: " << scaled_value << std::endl;

            // 根据 precision 选择存储类型
            using duckdb::Decimal;
            if (precision <= Decimal::MAX_WIDTH_INT16) {
                add(static_cast<int16_t>(scaled_value));
            } else if (precision <= Decimal::MAX_WIDTH_INT32) {
                add(static_cast<int32_t>(scaled_value));
            } else if (precision <= Decimal::MAX_WIDTH_INT64) {
                add(scaled_value);
            } else {
                // 如果 precision > INT64
                add(static_cast<int64_t>(scaled_value));  // 使用 INT64 类型作为默认
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "[ERROR] Exception in add(std::string &value): " << e.what() << std::endl;
        throw;
    }

    std::cout << "[DEBUG] Exiting add(std::string &value)" << std::endl;
}




void DecimalColumnVector::add(bool value) {
    std::cout << "[DEBUG] Entered add(bool value), input: " << value << std::endl;
    add(value ? 1 : 0);
    std::cout << "[DEBUG] Exiting add(bool value)" << std::endl;
}

void DecimalColumnVector::add(int64_t value) {
    std::cout << "[DEBUG] Entered add(int64_t value), input: " << value << std::endl;

    if (writeIndex >= length) {
        std::cout << "[DEBUG] Current writeIndex (" << writeIndex 
                  << ") exceeds or equals length (" << length << "). Resizing..." << std::endl;
        ensureSize(writeIndex * 2, true);
    }

    int index = writeIndex++;
    vector[index] = value;
    isNull[index] = false;

    std::cout << "[DEBUG] Stored value at index " << index 
              << ". Current writeIndex: " << writeIndex << std::endl;
    std::cout << "[DEBUG] Exiting add(int64_t value)" << std::endl;
}

void DecimalColumnVector::add(int value) {
    std::cout << "[DEBUG] Entered add(int value), input: " << value << std::endl;
    add(static_cast<int64_t>(value));
    std::cout << "[DEBUG] Exiting add(int value)" << std::endl;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    std::cout << "[DEBUG] Entered ensureSize(size: " << size 
              << ", preserveData: " << std::boolalpha << preserveData << ")" << std::endl;

    ColumnVector::ensureSize(size, preserveData);
    if (length >= size) {
        std::cout << "[DEBUG] Current length (" << length << ") is already >= requested size (" << size << ")." << std::endl;
        return;
    }

    long *oldVector = vector;
    std::cout << "[DEBUG] Resizing vector. Old length: " << length 
              << ", New size: " << size << std::endl;

    posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(int64_t));
    if (preserveData && oldVector) {
        std::cout << "[DEBUG] Copying data from old vector to new vector." << std::endl;
        std::copy(oldVector, oldVector + length, vector);
    }

    delete[] oldVector;
    memoryUsage += static_cast<long>(sizeof(long) * (size - length));
    resize(size);

    std::cout << "[DEBUG] Exiting ensureSize(size: " << size 
              << ", preserveData: " << std::boolalpha << preserveData << ")" << std::endl;
}
int DecimalColumnVector::getPrecision()
{
    return precision;
}

int DecimalColumnVector::getScale() {
	return scale;
}
