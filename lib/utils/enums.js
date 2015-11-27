/**
 * Copyright 2015 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an 
 BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/

/**
 * Enum for Kafka protocol values.
 * @enum {string}
 */
exports.KAFKA_PROTOCOL = {
    PLAINTEXT: "PLAINTEXT",
    SSL: "SSL",
    SASL_PLAINTEXT: "SASL_PLAINTEXT",
    SASL_SSL: "SASL_SSL"
    
};

/**
 * Enum for Kafka defaults
 * @enum {*}
 */
exports.KAFKA_DEFAULTS = {
    HOST: "localhost",
    PORT: 9092,
    SSL_PORT: 9093,
    PROTOCOL: this.KAFKA_PROTOCOL.PLAINTEXT
};

