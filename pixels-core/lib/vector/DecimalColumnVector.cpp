//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"

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
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
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
    
    // 获取精度和标度
    int precision = getPrecision();
    int scale = getScale();

    // 解析字符串
    bool is_negative = false;
    size_t dot_position = value.find('.');

    if (value[0] == '-') {
        is_negative = true;
        value = value.substr(1); // 移除负号
    }

    // 整数部分和小数部分的提取
    std::string integer_part = (dot_position == std::string::npos) ? value : value.substr(0, dot_position);
    std::string fractional_part = (dot_position == std::string::npos) ? "" : value.substr(dot_position + 1);

    // 确保小数部分长度符合标度
    if (fractional_part.length() > static_cast<size_t>(scale)) {
        throw std::invalid_argument("Scale exceeds the defined precision: " + value);
    }

    // 补充小数部分至标度长度
    fractional_part.append(scale - fractional_part.length(), '0');

    // 合并整数部分和小数部分
    std::string full_number = integer_part + fractional_part;

    // 转换为整数类型
    int64_t stored_value = std::stoll(full_number);

    if (is_negative) {
        stored_value = -stored_value;
    }

    // 将转换后的整数存储到向量
    add(stored_value);
}

void DecimalColumnVector::add(bool value) {
    add(value ? 1 : 0);
}

void DecimalColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }

    if (physical_type_ == PhysicalType::INT16) {
        reinterpret_cast<int16_t *>(vector)[writeIndex] = static_cast<int16_t>(value);
    } else if (physical_type_ == PhysicalType::INT32) {
        reinterpret_cast<int32_t *>(vector)[writeIndex] = static_cast<int32_t>(value);
    } else if (physical_type_ == PhysicalType::INT64) {
        reinterpret_cast<int64_t *>(vector)[writeIndex] = value;
    } else {
        throw std::runtime_error("Unsupported physical type for decimal storage");
    }
    writeIndex++;
}

void DecimalColumnVector::add(int value) {
    add(static_cast<int64_t>(value));
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    if (length >= size) {
        return;
    }

    void *old_vector = vector;
    uint64_t old_length = length;
    length = size;

    if (physical_type_ == PhysicalType::INT16) {
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(int16_t));
        if (preserveData && old_vector) {
            std::copy(reinterpret_cast<int16_t *>(old_vector),
                      reinterpret_cast<int16_t *>(old_vector) + old_length,
                      reinterpret_cast<int16_t *>(vector));
        }
    } else if (physical_type_ == PhysicalType::INT32) {
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(int32_t));
        if (preserveData && old_vector) {
            std::copy(reinterpret_cast<int32_t *>(old_vector),
                      reinterpret_cast<int32_t *>(old_vector) + old_length,
                      reinterpret_cast<int32_t *>(vector));
        }
    } else if (physical_type_ == PhysicalType::INT64) {
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(int64_t));
        if (preserveData && old_vector) {
            std::copy(reinterpret_cast<int64_t *>(old_vector),
                      reinterpret_cast<int64_t *>(old_vector) + old_length,
                      reinterpret_cast<int64_t *>(vector));
        }
    } else {
        throw std::runtime_error("Unsupported physical type for decimal storage");
    }

    if (old_vector) {
        free(old_vector);
    }
}

int DecimalColumnVector::getPrecision()
{
    return precision;
}

int DecimalColumnVector::getScale() {
	return scale;
}
