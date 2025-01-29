class XMLDeserializerHelper:
    yes_options = ["yes", "1", "X", "ok", "Y"]

    @staticmethod
    def determine_yes_no_value(value):
        """
        Returns True if the value matches one of the 'yes' options (yes_options).
        Returns False if the value does not match.
        Returns None if the value is None.
        """
        if value is None:
            return None

        return any(option.lower() == value.lower() for option in XMLDeserializerHelper.yes_options)

    @staticmethod
    def determine_yes_no_with_null_value(value):
        """
        Returns True if the value matches one of the 'yes' options (yes_options).
        Returns False if the value does not match.
        Returns None if the value is 'NULL' or None.
        """
        if value is None:
            return None

        if value.lower() == "null":
            return None

        return XMLDeserializerHelper.determine_yes_no_value(value)

    class YesNoKa:
        Yes = "Yes"
        No = "No"
        KA = "KA"

    @staticmethod
    def determine_yes_no_ka_value(value):
        """
        Returns 'Yes' if the value matches one of the 'yes' options (yes_options).
        Returns 'No' if the value does not match.
        Returns 'KA' if the value is 'NULL'.
        Returns None if the value is None.
        """
        if value is None:
            return None

        if value.lower() == "null":
            return XMLDeserializerHelper.YesNoKa.KA

        return XMLDeserializerHelper.YesNoKa.Yes if XMLDeserializerHelper.determine_yes_no_value(
            value) else XMLDeserializerHelper.YesNoKa.No

    @staticmethod
    def determine_yes_no_ka_value_as_int(value):
        """
        Returns 'Yes' if the value matches one of the 'yes' options (yes_options).
        Returns 'No' if the value does not match.
        Returns 'KA' if the value is 'NULL'.
        Returns None if the value is None.
        """
        if value is None or value.lower() == "null":
            return None

        return 1 if XMLDeserializerHelper.determine_yes_no_value(value) else 0
