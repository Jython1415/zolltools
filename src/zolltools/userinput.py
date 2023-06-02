"""Module for getting input from the user"""

import math

def get_integer(
    lower_bound_incl,
    upper_bound_excl,
    prompt="\n:",
    value_error_message="Not an integer; Try again.",
    out_of_bounds_message="Input is out of bounds; Try again.",
) -> int:
    """Returns the integer selected by the user.
    The integer will be within the range [lower_bound_incl, upper_bound_excl)
    """
    no_value_selected = True
    while no_value_selected:
        try:
            user_input = int(input(prompt))
        except ValueError:
            print(value_error_message)
        else:
            if user_input >= lower_bound_incl and user_input < upper_bound_excl:
                no_value_selected = False
            else:
                print(out_of_bounds_message)
    return user_input

def get_yes_or_no(prompt="\n:", invalid_input_message=None) -> int:
    """Return values: -1 for error, 0 or no, 1 for yes"""

    if invalid_input_message is None:
        invalid_input_message = (
            "Input could not be resolved to 'y' or 'n'. Try again."
        )

    no_response_selected = True
    while no_response_selected:
        user_input = input(prompt)

        if user_input == "y" or user_input == "Y":
            return 1
        elif user_input == "n" or user_input == "N":
            return 0
        else:
            print(invalid_input_message)
    return -1

def select_from_list(
    selection_list,
    print_result=True,
    prompt="\n:",
    value_error_message="Not an integer; Try again.",
    out_of_bounds_message="Input is out of bounds; Try again.",
):
    """Returns the items from the list that the user selected
    The list should contain items that have an implementation of __str__
    """

    num_digits = math.ceil(math.log10(len(selection_list)))
    for index, item in enumerate(selection_list):
        print(f"{str(index + 1).zfill(num_digits)} {str(item)}")

    upper_bound = len(selection_list) + 1
    selected_index = -1 + get_integer(
        1, upper_bound, prompt, value_error_message, out_of_bounds_message
    )
    selection = selection_list[selected_index]

    if print_result:
        print(selection)
    return selection
