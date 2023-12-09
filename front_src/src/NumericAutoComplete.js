import React from 'react'
import { AutoComplete } from 'antd'

const searchResult = async (api, token, query, by) => {
  if (query) {
    return fetch(`${api}contragents/?token=${token}&${by}=${query}`)
      .then((response) => response.json())
      .then((body) =>
        body.result.map((user) => ({
          label: by === "phone" ? `${user.phone}` : `${user.inn}`,
          value: user.id,
        })),
      )
      .then((data) => data.filter((ca) => ca.label !== "null"))
  }
  else {
    return fetch(`${api}contragents/?token=${token}`)
      .then((response) => response.json())
      .then((body) =>
        body.result.map((user) => ({
          label: by === "phone" ? `${user.phone}` : `${user.inn}`,
          value: user.id,
        })),
      )
      .then((data) => data.filter((ca) => ca.label !== "null"))
  }
}

class NumericAutoComplete extends React.Component {
  constructor(props) {
    super(props)

    this.state = {
      options: this.props.options,
    }
  }


  handleSearch = async (value) => {
    this.setState({
      options: await searchResult(this.props.api, this.props.token, value, this.props.by)
    })
  }

  render() {
    const { options } = this.state
    return (
      <AutoComplete
        {...this.props}
        options={options}
        onSearch={this.handleSearch}
      />
    )
  }
}

export default NumericAutoComplete