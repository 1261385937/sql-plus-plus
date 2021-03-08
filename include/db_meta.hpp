#pragma once
#include <type_traits>
#include <tuple>
#include <optional>

namespace sqlcpp {
	template<template<typename...> class Template, typename T>
	struct is_specialization_of :std::false_type {};

	template<template<typename...> class Template, typename ...Args>
	struct is_specialization_of<Template, Template<Args...>> :std::true_type {};

	template<typename T>
	struct is_tuple :is_specialization_of<std::tuple, std::decay_t<T>> {};

	template <typename T>
	inline constexpr bool is_tuple_v = is_tuple<T>::value;

	template<typename T>
	struct is_optional :is_specialization_of<std::optional, std::decay_t<T>> {};

	template <typename T>
	inline constexpr bool is_optional_v = is_optional<T>::value;

	template<typename T>
	inline constexpr bool is_char_array_v =
		std::is_array_v<T> && std::is_same_v<char, std::remove_cv_t<std::remove_pointer_t<std::remove_reference_t<std::decay_t<T>>>>>;

	template<typename T>
	inline constexpr bool is_char_pointer_v =
		std::is_pointer_v<T> && std::is_same_v<char, std::remove_cv_t<std::remove_pointer_t<std::remove_reference_t<T>>>>;

	template <typename>
	inline constexpr bool always_false_v = false;

	template<bool Cond, typename T1, typename T2>
	struct return_if {};

	template<typename T1, typename T2>
	struct return_if<true, T1, T2> {
		using type = T1;
	};

	template<typename T1, typename T2>
	struct return_if<false, T1, T2> {
		using type = T2;
	};

	template<bool Cond, typename T1, typename T2>
	using return_if_t = typename return_if<Cond, T1, T2>::type;

	template<typename T, typename T1 = void>
	struct has_char_array :std::false_type {};

	template<typename T>
	struct has_char_array<T, std::enable_if_t<is_char_array_v<T>, void>> :std::true_type {};

	template<typename...T>
	inline constexpr bool is_has_char_array_v = std::disjunction_v<has_char_array<T>...>;

	
}