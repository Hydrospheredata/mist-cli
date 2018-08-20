# coding=utf-8

from mistpy.decorators import on_spark_context, with_args, arg, list_type


@with_args(
    arg('numbers', type_hint=list_type(int)),
    arg('multiplier', type_hint=float, default=2)
)
@on_spark_context
def my_custom_fn(sc, numbers, multiplier):
    rdd = sc.parallelize(numbers)
    result = rdd.map(lambda s: s * multiplier).collect()
    return {"result": result}
