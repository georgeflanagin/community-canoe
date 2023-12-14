def sequence(target:int, numbers:tuple) -> tuple:
    yield from ( (i, target-i) 
        for i in numbers
            if target-i in numbers and target>=i )

